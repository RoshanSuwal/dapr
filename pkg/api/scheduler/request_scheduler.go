package scheduler

import (
	"context"
	"encoding/json"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/redis"
	"github.com/dapr/kit/logger"
	"go.uber.org/zap"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

type RequestSchedulerOpts struct {
	MaxWorker               int
	RequestChanCapacity     int
	Worker                  int
	RequestSchedulingPolicy string

	RedisHost     string
	RedisDatabase string
	RedisPassword string

	BudgetConfigPath string
	DefaultBudget    int

	EnableScheduling bool
	LoggerName       string

	EnableLogging   bool
	LoggingInterval int
}

type ScRequest struct {
	Method              string
	Endpoint            string
	Service             string
	RequestTimestamp    int64
	Budget              int64
	QueuingDelay        int64
	ServiceTime         int64
	CompletionTimestamp time.Time
	ServiceSig          chan struct{}
	QueueSize           int
	ActiveConnections   int
	priority            int
	index               int
	UberTraceId         string
	TraceId             string
	RemainingBudget     int64
	RID                 string
}

type EndpointBudget struct {
	Service  string `json:"service"`
	Endpoint string `json:"endpoint"`
	Method   string `json:"method"`
	Budget   int64  `json:"budget"`
}

func key(method string, endpoint string) string {
	return method + " " + endpoint
}

type RequestScheduler struct {
	policy           SchedulingPolicy
	ScRequestChan    chan *ScRequest
	ScWorkerChan     chan struct{}
	activeWorkers    int64
	totalWorkers     int64
	logger           logger.Logger
	stateStore       state.Store
	ctx              context.Context
	enableScheduling bool
	enableLogging    bool
	loggingInterval  int
	defaultBudget    int64
	budgetPath       string
	budgets          map[string]EndpointBudget
}

func (s *RequestScheduler) upstream() {
	for r := range s.ScRequestChan {
		//	Fetch the budget and schedule only if no workers are available
		s.allocateBudget(r)
		s.policy.Enqueue(r, r.RequestTimestamp+r.Budget)
	}
}

func (s *RequestScheduler) downstream() {
	for {
		select {
		case <-s.ScWorkerChan:
			r := s.policy.Dequeue().(*ScRequest)
			r.QueuingDelay = time.Now().UnixMicro() - r.RequestTimestamp
			r.RemainingBudget = r.Budget - r.QueuingDelay
			r.ServiceSig <- struct{}{}
			s.updateBudget(r)
			s.updateActiveWorkers(1)
		}
	}
}

func (s *RequestScheduler) updateActiveWorkers(n int64) {
	atomic.AddInt64(&s.activeWorkers, n)
}

func (s *RequestScheduler) allocateBudget(r *ScRequest) {
	//Add the static budget if any first
	// If activeWorker < totalWorkers, then no need to fetch budget from budget server,
	// Just allocate budget to 0 as no budget is going to be used for this request
	if s.policy.Name() == "fifo" {
		r.Budget = 0
		return
	} else if s.policy.Name() == "edf" {
		// TODO: fetch budget from budget server
		if endpointBudget, ok := s.budgets[key(r.Method, r.Endpoint)]; ok {
			//fmt.Printf("Returning from endpoint budget map!!\n")
			r.Budget = endpointBudget.Budget
		} else {
			r.Budget = s.defaultBudget
		}

		if s.activeWorkers >= s.totalWorkers {
			st, err := s.stateStore.Get(s.ctx, &state.GetRequest{Key: r.RID})
			if err != nil {
				r.Budget += 0
			} else {
				b, err := strconv.Atoi(string(st.Data))
				if err != nil {
					r.Budget += 0
				} else {
					r.Budget += int64(b)
				}
			}
		}
	}
	return
}

func (s *RequestScheduler) updateBudget(r *ScRequest) {
	if s.policy.Name() == "edf" {
		if r.RemainingBudget > 0 {
			//	TODO: update budget to budget server
			err := s.stateStore.Set(s.ctx, &state.SetRequest{Key: r.RID,
				Value: r.RemainingBudget,
				Metadata: map[string]string{
					"ttlInSeconds": "20",
				},
			})
			if err != nil {
				s.logger.Error("failed to set remaining budget", zap.Error(err))
			}
		}
	}
}

func (s *RequestScheduler) RegisterRequest(r *ScRequest) {
	s.ScRequestChan <- r
}

func (s *RequestScheduler) RegisterWorker() {
	select {
	case s.ScWorkerChan <- struct{}{}:
		s.updateActiveWorkers(-1)
	default:
	}
}

func (s *RequestScheduler) UpdateWorkers(allocateWorkers int64) {
	for s.totalWorkers < allocateWorkers {
		s.ScWorkerChan <- struct{}{}
		s.totalWorkers++
	}
	for s.totalWorkers > allocateWorkers {
		<-s.ScWorkerChan
		s.totalWorkers--
	}
}

func (s *RequestScheduler) loadBudgets() {
	file, err := os.Open(s.budgetPath)
	if err != nil {
		s.logger.Error("failed to open budget file", zap.Error(err))
		return
	}
	defer file.Close()

	var endpointBudgets []EndpointBudget

	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&endpointBudgets); err != nil {
		s.logger.Error("failed to decode budget file", zap.Error(err))
		return
	}

	for _, endpointBudget := range endpointBudgets {
		s.budgets[key(endpointBudget.Method, endpointBudget.Endpoint)] = endpointBudget
	}
	return
}

func (s *RequestScheduler) Run() {
	// load the budget first
	s.logger.Info("Loading the budget from ", s.budgetPath, " with default budget=", s.defaultBudget)
	s.loadBudgets()
	//
	// register the workers
	//Running the upstream
	s.logger.Info("Starting the scheduler Upstream")
	go s.upstream()
	s.logger.Info("Starting the scheduler Downstream")
	go s.downstream()
	if s.enableLogging {
		s.logger.Info("Starting the scheduler Logging with interval ", s.loggingInterval, " seconds")
		go func() {
			ticker := time.NewTicker(time.Duration(s.loggingInterval) * time.Second)
			defer ticker.Stop()
			for _ = range ticker.C {
				s.logger.Info("RequestScheduler: Total workers = ", s.totalWorkers, " Active workers = ", s.activeWorkers)
			}
		}()
	}

}

func newRequestScheduler(policy SchedulingPolicy, maxWorkers int64, requestChannelSize int64, logger logger.Logger, store state.Store, ctx context.Context, enableScheduling bool, enableLogging bool, loggingInterval int, defaultBudget int64, budgetPath string) *RequestScheduler {
	return &RequestScheduler{
		policy:           policy,
		totalWorkers:     0,
		activeWorkers:    0,
		ScWorkerChan:     make(chan struct{}, maxWorkers),
		ScRequestChan:    make(chan *ScRequest, requestChannelSize),
		logger:           logger,
		stateStore:       store,
		ctx:              ctx,
		enableScheduling: enableScheduling,
		enableLogging:    enableLogging,
		loggingInterval:  loggingInterval,
		defaultBudget:    defaultBudget,
		budgetPath:       budgetPath,
		budgets:          make(map[string]EndpointBudget),
	}
}

func NewRequestSchedulerFromConfig(opts RequestSchedulerOpts) *RequestScheduler {
	if !opts.EnableScheduling {
		return &RequestScheduler{enableScheduling: false}
	}

	_logger := logger.NewLogger(opts.LoggerName)
	redisOpts := map[string]string{
		"redisHost": opts.RedisHost,
		"database":  opts.RedisDatabase,
		"password":  opts.RedisPassword,
	}

	stateStore := redis.NewRedisStateStore(_logger)
	ctx := context.Background()
	if err := stateStore.Init(ctx, state.Metadata{Base: metadata.Base{Properties: redisOpts}}); err != nil {
		_logger.Info("redis state store init failed ", err.Error())
	}

	scheduler := newRequestScheduler(NewPolicy(opts.RequestSchedulingPolicy), int64(opts.MaxWorker), int64(opts.RequestChanCapacity), _logger, stateStore, ctx, opts.EnableLogging, opts.EnableLogging, opts.LoggingInterval, int64(opts.DefaultBudget), opts.BudgetConfigPath)
	scheduler.UpdateWorkers(int64(opts.Worker))
	return scheduler
}

func NewRequestScheduler(policyName string, maxWorkers int64, requestChannelSize int64) *RequestScheduler {

	// create a state store and pass it
	_logger := logger.NewLogger("RequestScheduler")

	redisOpts := map[string]string{
		"redisHost": "127.0.0.1:6379", // default
		"database":  "0",
	}
	redisStateStore := redis.NewRedisStateStore(_logger)
	ctx := context.Background()
	err := redisStateStore.Init(ctx, state.Metadata{
		Base: metadata.Base{Properties: redisOpts},
	})
	if err != nil {
		_logger.Info("redis state store init failed ", err.Error())
	}

	requestScheduler := newRequestScheduler(
		NewPolicy(policyName),
		maxWorkers,
		requestChannelSize,
		_logger,
		redisStateStore,
		ctx,
		true, true, 30, 0, "",
	)
	requestScheduler.UpdateWorkers(100)
	return requestScheduler
}
