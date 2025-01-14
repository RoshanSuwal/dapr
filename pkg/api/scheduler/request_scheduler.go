package scheduler

import (
	"context"
	"encoding/json"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/redis"
	diagUtils "github.com/dapr/dapr/pkg/diagnostics/utils"
	"github.com/dapr/kit/logger"
	"github.com/prometheus/client_golang/prometheus"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"go.uber.org/zap"
	"os"
	"runtime"
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

	BudgetConfigPath     string
	DefaultBudget        int
	EnableBudgetTransfer bool
	BudgetTTL            int

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
	Priority            int64
	QueuingDelay        int64
	ServiceTime         int64
	CompletionTimestamp time.Time
	ServiceSig          chan struct{}
	QueueSize           int
	ActiveConnections   int
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

// SchedulerMetrics Scheduler Monitoring
var (
	appIDKey                 = tag.MustNewKey("app_id")
	KeyMethod                = tag.MustNewKey("method")
	KeyEndpoint              = tag.MustNewKey("endpoint")
	KeyBudgetViolationStatus = tag.MustNewKey("budget_violation_status")
)

type schedulerMetricsMonitoring struct {
	appId                        string
	enabled                      bool
	queuingDelay                 *stats.Float64Measure
	serviceTime                  *stats.Float64Measure
	responseTime                 *stats.Float64Measure
	queueSize                    *stats.Int64Measure
	budget                       *stats.Float64Measure
	serviceTimeResponseTimeRatio *stats.Float64Measure // service_time / response_time
}

func newSchedulerMetricsMonitoring() *schedulerMetricsMonitoring {
	return &schedulerMetricsMonitoring{
		queuingDelay: stats.Float64(
			"scheduler/queuing_delay",
			"Queuing delay per request",
			stats.UnitMilliseconds),
		serviceTime: stats.Float64(
			"scheduler/service_time",
			"service time per request",
			stats.UnitMilliseconds),
		responseTime: stats.Float64(
			"scheduler/response_time",
			"response time per request",
			stats.UnitMilliseconds),
		budget: stats.Float64(
			"scheduler/budget",
			"budget per request",
			stats.UnitMilliseconds),
		queueSize: stats.Int64(
			"scheduler/queuing_size",
			"Total requests in queue",
			stats.UnitBytes),
		serviceTimeResponseTimeRatio: stats.Float64(
			"scheduler/service_time_response_time_ratio",
			"Budget violation status",
			stats.UnitDimensionless),
		enabled: false,
	}
}

func (m *schedulerMetricsMonitoring) Init(appId string, latencyDistribution *view.Aggregation) error {
	m.appId = appId
	m.enabled = true

	return view.Register(
		diagUtils.NewMeasureView(m.queuingDelay, []tag.Key{appIDKey, KeyMethod, KeyEndpoint, KeyBudgetViolationStatus}, latencyDistribution),
		diagUtils.NewMeasureView(m.serviceTime, []tag.Key{appIDKey, KeyMethod, KeyEndpoint, KeyBudgetViolationStatus}, latencyDistribution),
		diagUtils.NewMeasureView(m.responseTime, []tag.Key{appIDKey, KeyMethod, KeyEndpoint, KeyBudgetViolationStatus}, latencyDistribution),
		diagUtils.NewMeasureView(m.budget, []tag.Key{appIDKey, KeyMethod, KeyEndpoint, KeyBudgetViolationStatus}, latencyDistribution),
		diagUtils.NewMeasureView(m.queueSize, []tag.Key{appIDKey, KeyMethod, KeyEndpoint, KeyBudgetViolationStatus}, latencyDistribution),
		diagUtils.NewMeasureView(m.serviceTimeResponseTimeRatio, []tag.Key{appIDKey, KeyMethod, KeyEndpoint, KeyBudgetViolationStatus}, view.Distribution(prometheus.LinearBuckets(0.0, 0.05, 21)...)),
	)
}

func (m *schedulerMetricsMonitoring) IsEnabled() bool {
	return m != nil && m.enabled
}

func (m *schedulerMetricsMonitoring) MonitorRequestDataFromScRequest(ctx context.Context, r *ScRequest) {
	m.MonitorRequest(
		ctx, r.Method, r.Endpoint,
		float64(r.QueuingDelay/1000),
		float64(r.ServiceTime/1000),
		float64((r.QueuingDelay+r.ServiceTime)/1000),
		int64(r.QueueSize),
		float64(r.Budget/1000),
	)
}

// MonitorRequest All time unit must be in milliseconds
func (m *schedulerMetricsMonitoring) MonitorRequest(ctx context.Context, method string, endpoint string, queuingDelay float64, serviceTime float64, responseTime float64, queueSize int64, budget float64) {
	if !m.IsEnabled() {
		return
	}

	budgetViolationStatus := "false"
	if budget > 0 && budget-queuingDelay < 0 {
		budgetViolationStatus = "true"
	}

	stats.RecordWithTags(ctx,
		diagUtils.WithTags(m.queueSize.Name(), appIDKey, m.appId, KeyMethod, method, KeyEndpoint, endpoint, KeyBudgetViolationStatus, budgetViolationStatus),
		m.queueSize.M(queueSize))

	stats.RecordWithTags(ctx,
		diagUtils.WithTags(m.queuingDelay.Name(), appIDKey, m.appId, KeyMethod, method, KeyEndpoint, endpoint, KeyBudgetViolationStatus, budgetViolationStatus),
		m.queuingDelay.M(queuingDelay))

	stats.RecordWithTags(ctx,
		diagUtils.WithTags(m.serviceTime.Name(), appIDKey, m.appId, KeyMethod, method, KeyEndpoint, endpoint, KeyBudgetViolationStatus, budgetViolationStatus),
		m.serviceTime.M(serviceTime))

	stats.RecordWithTags(ctx,
		diagUtils.WithTags(m.budget.Name(), appIDKey, m.appId, KeyMethod, method, KeyEndpoint, endpoint, KeyBudgetViolationStatus, budgetViolationStatus),
		m.budget.M(budget))

	stats.RecordWithTags(ctx,
		diagUtils.WithTags(m.responseTime.Name(), appIDKey, m.appId, KeyMethod, method, KeyEndpoint, endpoint, KeyBudgetViolationStatus, budgetViolationStatus),
		m.responseTime.M(responseTime))

	stats.RecordWithTags(ctx,
		diagUtils.WithTags(m.serviceTimeResponseTimeRatio.Name(), appIDKey, m.appId, KeyMethod, method, KeyEndpoint, endpoint, KeyBudgetViolationStatus, budgetViolationStatus),
		m.serviceTimeResponseTimeRatio.M(serviceTime/responseTime))
}

func key(method string, endpoint string) string {
	return method + " " + endpoint
}

var _logger = logger.NewLogger("dapr.runtime.request_scheduler.metrics")
var log = logger.NewLogger("dapr.runtime.request_scheduler.info")

type RequestScheduler struct {
	policy                    SchedulingPolicy
	ScRequestChan             chan *ScRequest
	ScWorkerChan              chan struct{}
	activeWorkers             int64
	totalWorkers              int64
	Logger                    logger.Logger
	stateStore                state.Store
	ctx                       context.Context
	EnableScheduling          bool
	enableLogging             bool
	loggingInterval           int
	defaultBudget             int64
	budgetTTL                 int
	EnableBudgetTransfer      bool
	budgetPath                string
	budgets                   map[string]EndpointBudget
	SchedulerMetricMonitoring *schedulerMetricsMonitoring
}

func (s *RequestScheduler) upstream() {
	for r := range s.ScRequestChan {
		//	Fetch the budget and schedule only if no workers are available
		log.Info("[Registering request]", r.RID)
		r.QueueSize = s.policy.length()
		s.allocateBudget(r)
		s.policy.Enqueue(r, r.Priority)
	}
}

func (s *RequestScheduler) downstream() {
	for {
		select {
		case <-s.ScWorkerChan:
			r := s.policy.Dequeue().(*ScRequest)
			r.QueuingDelay = time.Now().UnixMicro() - r.RequestTimestamp
			r.RemainingBudget = r.Budget - r.QueuingDelay
			log.Info("[Dispatching request]", r.RID)
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
		r.Priority = 0
		return
	} else if s.policy.Name() == "edf" {
		// TODO: fetch budget from budget server
		if endpointBudget, ok := s.budgets[key(r.Method, r.Endpoint)]; ok {
			//fmt.Printf("Returning from endpoint budget map!!\n")
			r.Budget = endpointBudget.Budget
		} else {
			r.Budget = s.defaultBudget
		}

		if s.EnableBudgetTransfer && s.activeWorkers >= s.totalWorkers {
			log.Info("Fetching Budget")
			st, err := s.stateStore.Get(s.ctx, &state.GetRequest{Key: r.RID})
			if err != nil {
				r.Budget += 1
			} else {
				b, err := strconv.Atoi(string(st.Data))
				if err != nil {
					r.Budget += 1
				} else {
					r.Budget += int64(b)
				}
			}
		}

		r.Priority = r.RequestTimestamp + r.Budget
		return
	} else if s.policy.Name() == "rat" { // request arrival time
		st, err := s.stateStore.Get(s.ctx, &state.GetRequest{Key: r.RID})
		if err != nil {
			r.Budget += 0
			r.Priority = r.RequestTimestamp
		} else {
			b, err := strconv.Atoi(string(st.Data))
			if err != nil {
				r.Budget += 0
				r.Priority = r.RequestTimestamp
			} else {
				r.Budget = int64(b)
				r.Priority = int64(b)
			}
		}
	} else if s.policy.Name() == "pq" { // priority queue
		if endpointBudget, ok := s.budgets[key(r.Method, r.Endpoint)]; ok {
			//fmt.Printf("Returning from endpoint budget map!!\n")
			r.Budget = endpointBudget.Budget
		} else {
			r.Budget = s.defaultBudget
		}

		//if s.activeWorkers >= s.totalWorkers {
		st, err := s.stateStore.Get(s.ctx, &state.GetRequest{Key: r.RID})
		if err != nil {
			r.Budget += 0
		} else {
			b, err := strconv.Atoi(string(st.Data))
			if err != nil {
				r.Budget += 0
			} else {
				r.Budget = int64(b)
			}
		}
		//}
		r.Priority = r.RequestTimestamp + r.Budget*10000000000000000 // 1730839889875183
	}
	return
}

func (s *RequestScheduler) updateBudget(r *ScRequest) {
	if s.policy.Name() == "edf" {
		if r.RemainingBudget > 0 && s.EnableBudgetTransfer {
			log.Info("Transferring budget")
			//	TODO: update budget to budget server
			err := s.stateStore.Set(s.ctx, &state.SetRequest{Key: r.RID,
				Value: r.RemainingBudget,
				Metadata: map[string]string{
					"ttlInSeconds": strconv.Itoa(s.budgetTTL),
				},
			})
			if err != nil {
				s.Logger.Error("failed to set remaining budget", zap.Error(err))
			}
		}
	} else if s.policy.Name() == "rat" {
		if r.Priority == r.RequestTimestamp {
			err := s.stateStore.Set(s.ctx, &state.SetRequest{Key: r.RID,
				Value: r.RequestTimestamp,
				Metadata: map[string]string{
					"ttlInSeconds": strconv.Itoa(s.budgetTTL),
				},
			})
			if err != nil {
				s.Logger.Error("failed to set remaining budget", zap.Error(err))
			}
		}
	} else if s.policy.Name() == "pq" {
		if r.Budget != s.defaultBudget {
			err := s.stateStore.Set(s.ctx, &state.SetRequest{Key: r.RID,
				Value: r.Budget,
				Metadata: map[string]string{
					"ttlInSeconds": strconv.Itoa(s.budgetTTL),
				},
			})
			if err != nil {
				s.Logger.Error("failed to set remaining budget", zap.Error(err))
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

func (s *RequestScheduler) LogMetrics(r *ScRequest) {
	s.Logger.WithFields(map[string]any{
		"method":           r.Method,
		"endpoint":         r.Endpoint,
		"queuing_delay":    r.QueuingDelay,
		"service_time":     r.ServiceTime,
		"budget":           r.Budget,
		"remaining_budget": r.RemainingBudget,
		"RID":              r.RID,
		"response_time":    r.ServiceTime + r.QueuingDelay,
		"service":          r.Service,
		"priority":         r.Priority,
		"arrival_time":     r.RequestTimestamp,
		"queue_size":       r.QueueSize,
	}).Info("request.scheduler")
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
	log.Info("Loading budget from ", s.budgetPath)
	file, err := os.Open(s.budgetPath)
	if err != nil {
		s.Logger.Error("failed to open budget file", zap.Error(err))
		return
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
		}
	}(file)

	var endpointBudgets []EndpointBudget

	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&endpointBudgets); err != nil {
		s.Logger.Error("failed to decode budget file", zap.Error(err))
		return
	}

	for _, endpointBudget := range endpointBudgets {
		s.budgets[key(endpointBudget.Method, endpointBudget.Endpoint)] = endpointBudget
	}
	return
}

func (s *RequestScheduler) Run() {

	if !s.EnableScheduling {
		s.Logger.Info("request scheduler is disabled")
		return
	}

	s.Logger.
		WithFields(map[string]any{
			"N":                    s.totalWorkers,
			"policy":               s.policy.Name(),
			"EnableScheduling":     s.EnableScheduling,
			"enableLogging":        s.enableLogging,
			"loggingInterval":      s.loggingInterval,
			"defaultBudget":        s.defaultBudget,
			"budgetPath":           s.budgetPath,
			"enableBudgetTransfer": s.EnableBudgetTransfer,
			"budgetTTL":            s.budgetTTL,
		}).
		Info("Running Request scheduler")

	// load the budget first
	log.Info("Loading the budget from ", s.budgetPath, " with default budget=", s.defaultBudget)
	s.loadBudgets()
	//
	// register the workers
	//Running the upstream
	log.Info("Starting the scheduler Upstream")
	go s.upstream()
	log.Info("Starting the scheduler Downstream")
	go s.downstream()
	if s.enableLogging {
		log.Info("Starting the scheduler Logging with interval ", s.loggingInterval, " seconds")
		go func() {
			ticker := time.NewTicker(time.Duration(s.loggingInterval) * time.Second)
			defer ticker.Stop()
			for _ = range ticker.C {
				log.WithFields(map[string]any{
					"total_workers":   s.totalWorkers,
					"active_workers":  s.activeWorkers,
					"num_go_routines": runtime.NumGoroutine(),
				}).Info("worker_stats")
			}
		}()
	}

}

func newRequestScheduler(policy SchedulingPolicy, maxWorkers int64, requestChannelSize int64, logger logger.Logger, store state.Store, ctx context.Context, enableScheduling bool, enableLogging bool, loggingInterval int, defaultBudget int64, budgetPath string, enableBudgetTransfer bool, budgetTTL int) *RequestScheduler {
	return &RequestScheduler{
		policy:                    policy,
		totalWorkers:              0,
		activeWorkers:             0,
		ScWorkerChan:              make(chan struct{}, maxWorkers),
		ScRequestChan:             make(chan *ScRequest, requestChannelSize),
		Logger:                    logger,
		stateStore:                store,
		ctx:                       ctx,
		EnableScheduling:          enableScheduling,
		enableLogging:             enableLogging,
		loggingInterval:           loggingInterval,
		defaultBudget:             defaultBudget,
		budgetPath:                budgetPath,
		budgets:                   make(map[string]EndpointBudget),
		EnableBudgetTransfer:      enableBudgetTransfer,
		budgetTTL:                 budgetTTL,
		SchedulerMetricMonitoring: newSchedulerMetricsMonitoring(),
	}
}

func NewRequestSchedulerFromConfig(opts RequestSchedulerOpts) *RequestScheduler {
	ctx := context.Background()
	if !opts.EnableScheduling {
		return &RequestScheduler{EnableScheduling: false, enableLogging: false, ctx: ctx, Logger: _logger}
	}

	//_logger := logger.NewLogger(opts.LoggerName)
	redisOpts := map[string]string{
		"redisHost":     opts.RedisHost,
		"database":      opts.RedisDatabase,
		"redisPassword": opts.RedisPassword,
	}

	stateStore := redis.NewRedisStateStore(_logger)
	if err := stateStore.Init(ctx, state.Metadata{Base: metadata.Base{Properties: redisOpts}}); err != nil {
		_logger.Info("redis state store init failed ", err.Error())
	}

	scheduler := newRequestScheduler(
		NewPolicy(opts.RequestSchedulingPolicy),
		int64(opts.MaxWorker),
		int64(opts.RequestChanCapacity),
		_logger,
		stateStore,
		ctx,
		opts.EnableScheduling,
		opts.EnableLogging,
		opts.LoggingInterval,
		int64(opts.DefaultBudget),
		opts.BudgetConfigPath,
		opts.EnableBudgetTransfer,
		opts.BudgetTTL,
	)
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
		true, true, 30, 0, "", true, 20,
	)

	requestScheduler.UpdateWorkers(100)
	return requestScheduler
}
