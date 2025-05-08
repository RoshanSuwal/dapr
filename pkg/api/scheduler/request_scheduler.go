package scheduler

import (
	"context"
	"encoding/json"
	"github.com/dapr/components-contrib/metadata"
	nr "github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/redis"
	invokev1 "github.com/dapr/dapr/pkg/messaging/v1"
	"github.com/dapr/kit/logger"
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

	EnableScaling        bool
	ScalingServerAddress string
	ScalingServerPort    int
	TargetAppId          string
	LoadBalancingPolicy  string
	Replica              int
}

type ScRequest struct {
	Method              string
	Endpoint            string
	Service             string
	RequestTimestamp    int64
	Budget              int64
	CumulativeBudget    int64 // allocated_budget + credited_budget
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

func key(method string, endpoint string) string {
	return method + " " + endpoint
}

var _logger = logger.NewLogger("dapr.runtime.request_scheduler.metrics")
var log = logger.NewLogger("dapr.runtime.request_scheduler.info")

type RequestScheduler struct {
	appId                      string
	policy                     SchedulingPolicy
	ScRequestChan              chan *ScRequest
	ScWorkerChan               chan struct{}
	activeWorkers              int64
	totalWorkers               int64
	Logger                     logger.Logger
	stateStore                 state.Store
	ctx                        context.Context
	EnableScheduling           bool
	enableLogging              bool
	loggingInterval            int
	defaultBudget              int64
	budgetTTL                  int
	EnableBudgetTransfer       bool
	budgetPath                 string
	budgets                    map[string]EndpointBudget
	SchedulerMetricMonitoring  *schedulerMetricsMonitoring
	scalingMetricsMonitoring   *ScalingMetricsMonitoring
	scalingMetricReportSigChan chan struct{}
	scalingConfiguration       ScalingConfiguration
	enableScaling              bool
	TargetAppId                string

	remoteInvokeFn  func(ctx context.Context, id string, namespace string, cacheKey string, address string, req *invokev1.InvokeMethodRequest) (*invokev1.InvokeMethodResponse, error)
	localInvokeFn   func(ctx context.Context, targetAppID string, req *invokev1.InvokeMethodRequest) (*invokev1.InvokeMethodResponse, error)
	getRemoteAppsFn func(appID string) (id string, namespace string, cacheKey string, addressList nr.AddressList, err error)
	loadBalancer    LoadBalancer
}

func (s *RequestScheduler) SetRemoteInvokeFn(remoteInvokeFn func(ctx context.Context, id string, namespace string, cacheKey string, address string, req *invokev1.InvokeMethodRequest) (*invokev1.InvokeMethodResponse, error)) {
	s.remoteInvokeFn = remoteInvokeFn
}

func (s *RequestScheduler) SetLocalInvokeFn(localInvokeFn func(ctx context.Context, targetAppID string, req *invokev1.InvokeMethodRequest) (*invokev1.InvokeMethodResponse, error)) {
	s.localInvokeFn = localInvokeFn
}

func (s *RequestScheduler) SetGetRemoteAppsFn(getRemoteAppsFn func(appID string) (id string, namespace string, cacheKey string, addressList nr.AddressList, err error)) {
	s.getRemoteAppsFn = getRemoteAppsFn
}

func (s *RequestScheduler) SetLoadBalancer(loadBalancer LoadBalancer) {
	s.loadBalancer = loadBalancer
}

func (s *RequestScheduler) InvokeMethodFn(ctx context.Context, req *invokev1.InvokeMethodRequest) (resp *invokev1.InvokeMethodResponse, err error) {
	if s.TargetAppId == "" || s.TargetAppId == s.appId || s.scalingConfiguration.Replica == 1 {
		return s.localInvokeFn(ctx, s.TargetAppId, req)
	}

	// Get the list of addresses
	id, namespace, cacheKey, addressList, err := s.getRemoteAppsFn(s.TargetAppId)
	if err != nil {
		log.Warn(err.Error())
		return s.localInvokeFn(ctx, s.TargetAppId, req)
		//return nil, err
	}
	// Select the address based on least connection
	addressList = append(addressList, "localhost")
	address := s.loadBalancer.Select(addressList)
	// Add the localhost address to addressList as it also contains microservice of desired application
	s.loadBalancer.UpdateActiveConnections(address, 1)
	// select the appropriate invoke function
	log.WithFields(map[string]any{"selected": address, "addressList": addressList}).Info("Load balancer")
	if address == "localhost" || address == "" {
		resp, err = s.localInvokeFn(ctx, address, req)
	} else {
		resp, err = s.remoteInvokeFn(ctx, id, namespace, cacheKey, address, req)
	}
	s.loadBalancer.UpdateActiveConnections(address, -1)
	return resp, err
}

func (s *RequestScheduler) SetEnableScaling(enableScaling bool) {
	s.enableScaling = enableScaling
}

func (s *RequestScheduler) SetScalingMetricsMonitoring(scalingMetricsMonitoring *ScalingMetricsMonitoring) {
	s.scalingMetricsMonitoring = scalingMetricsMonitoring
	s.scalingMetricReportSigChan = make(chan struct{}, 1)
}

func (s *RequestScheduler) SetScalingConfiguration(scalingConfiguration ScalingConfiguration) {
	s.scalingConfiguration = scalingConfiguration
}

func (s *RequestScheduler) SetAppId(appId string) {
	s.appId = appId
	if s.enableScaling {
		s.scalingMetricsMonitoring.SetAppId(appId)
	}
}

func (s *RequestScheduler) reportToScalingMetricMonitoring(r *ScRequest) {
	s.scalingMetricsMonitoring.addMetric(r.Endpoint, r.CumulativeBudget, r.Budget, r.QueuingDelay, r.RequestTimestamp)
	// check if violation exceeds threshold or not
	if s.scalingMetricsMonitoring.checkBudgetViolation(s.scalingConfiguration) {
		select {
		case s.scalingMetricReportSigChan <- struct{}{}:
		default:
			log.Warn("Skipping scaling metric report signal: channel full")
		}
	}
}

func (s *RequestScheduler) scaling() {
	// contains logic to sync the scheduler configuration from bsd auto scalar
	// reports the budget_violation metrics to auto scalar
	// continuously send the health or sync request to auto scalar
	secondTicker := time.NewTicker(time.Second)
	defer secondTicker.Stop()
	secondTickerCounter := int64(0)
	for {
		select {
		case <-s.scalingMetricReportSigChan: // send budget violation channel
			log.Debug("Reporting to scalar")
			scalingReportResponse, err := s.scalingMetricsMonitoring.SendReportToScalar(s.ctx)
			if err != nil {
				log.Errorf(err.Error())
				continue
			}

			s.scalingConfiguration.MaxConcurrencyPerReplica = scalingReportResponse.MaxConcurrencyPerReplica
			s.scalingConfiguration.Replica = scalingReportResponse.Replica
			s.scalingConfiguration.ScalingCheckIntervalInSeconds = scalingReportResponse.ScalingCheckIntervalInSeconds
			s.scalingConfiguration.AllocatedBudgetViolationThreshold = scalingReportResponse.AllocatedBudgetViolationThreshold
			s.scalingConfiguration.CumulativeBudgetViolationThreshold = scalingReportResponse.CumulativeBudgetViolationThreshold

			s.scalingMetricsMonitoring.SetWindowPeriodInSec(scalingReportResponse.ScalingMetricWindowPeriodInSeconds)

			if s.scalingConfiguration.Replica*s.scalingConfiguration.MaxConcurrencyPerReplica != s.totalWorkers {
				s.UpdateWorkers(s.scalingConfiguration.Replica * s.scalingConfiguration.MaxConcurrencyPerReplica)
			}

		case t := <-secondTicker.C:
			log.Debug("Scaling Second Ticker :", t.String(), " - counter : ", secondTickerCounter)
			secondTickerCounter += 1
			s.scalingMetricsMonitoring.computeNewArrivalRate()
			if secondTickerCounter%s.scalingConfiguration.ScalingCheckIntervalInSeconds == 0 {
				// send signal to send scalingMetric to auto scalar
				select {
				case s.scalingMetricReportSigChan <- struct{}{}:
				default:
					log.Warn("Skipping scaling metric report signal: channel full")
				}
			}
		}
	}
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
			r.RemainingBudget = r.CumulativeBudget - r.QueuingDelay
			log.Info("[Dispatching request]", r.RID)
			r.ServiceSig <- struct{}{}
			s.updateBudget(r)
			s.updateActiveWorkers(1)
			if s.enableScaling {
				s.reportToScalingMetricMonitoring(r)
			}
		}
	}
}

func (s *RequestScheduler) updateActiveWorkers(n int64) {
	atomic.AddInt64(&s.activeWorkers, n)
}

func (s *RequestScheduler) allocateBudget(r *ScRequest) {
	//Add the static budget if any first
	// activeWorker < totalWorkers, then no need to fetch budget from budget server,
	// Just allocate budget to 0 as no budget is going to be used for this request
	if s.policy.Name() == "fifo" {
		r.Budget = 0
		r.Priority = 0
		r.CumulativeBudget = 0
		return
	} else if s.policy.Name() == "edf" {
		// TODO: fetch budget from budget server
		if endpointBudget, ok := s.budgets[key(r.Method, r.Endpoint)]; ok {
			//fmt.Printf("Returning from endpoint budget map!!\n")
			r.Budget = endpointBudget.Budget
		} else {
			r.Budget = s.defaultBudget
		}
		r.CumulativeBudget = r.Budget

		if s.EnableBudgetTransfer { // s.EnableBudgetTransfer && s.activeWorkers >= s.totalWorkers // need to compute the credited budget
			log.Info("Fetching Budget")
			st, err := s.stateStore.Get(s.ctx, &state.GetRequest{Key: r.RID})
			if err != nil {
				r.CumulativeBudget += 1
			} else {
				b, err := strconv.Atoi(string(st.Data))
				if err != nil {
					r.CumulativeBudget += 1
				} else {
					r.CumulativeBudget += int64(b)
				}
			}
		}

		r.Priority = r.RequestTimestamp + r.CumulativeBudget
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
		r.CumulativeBudget = r.Budget
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
		r.CumulativeBudget = r.Budget
	}
	return
}

func (s *RequestScheduler) updateBudget(r *ScRequest) {
	if s.policy.Name() == "edf" {
		if s.EnableBudgetTransfer { // r.RemainingBudget > 0 && s.EnableBudgetTransfer // allowing to transfer negative budget too
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
		"method":            r.Method,
		"endpoint":          r.Endpoint,
		"queuing_delay":     r.QueuingDelay,
		"service_time":      r.ServiceTime,
		"budget":            r.Budget,
		"cumulative_budget": r.CumulativeBudget,
		"remaining_budget":  r.RemainingBudget,
		"RID":               r.RID,
		"response_time":     r.ServiceTime + r.QueuingDelay,
		"service":           r.Service,
		"priority":          r.Priority,
		"arrival_time":      r.RequestTimestamp,
		"queue_size":        r.QueueSize,
	}).Info("request.scheduler")
}

func (s *RequestScheduler) UpdateWorkers(allocateWorkers int64) {
	log.WithFields(map[string]any{"from": s.totalWorkers, "to": allocateWorkers}).Info("Updating Dispatcher Workers")
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
			"enableScaling":        s.enableScaling,
			"TargetAppID":          s.TargetAppId,
			"AppId":                s.appId,
			"LoadBalancer":         s.loadBalancer.Type(),
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
			for range ticker.C {
				log.WithFields(map[string]any{
					"total_workers":   s.totalWorkers,
					"active_workers":  s.activeWorkers,
					"num_go_routines": runtime.NumGoroutine(),
				}).Info("worker_stats")
			}
		}()
	}

	if s.enableScaling {
		log.Info("Starting the scheduler Scaling")
		go s.scaling()
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
		enableScaling:             false,
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

	scheduler.TargetAppId = opts.TargetAppId
	scheduler.SetLoadBalancer(NewLoadBalancer(opts.LoadBalancingPolicy))
	scheduler.SetEnableScaling(opts.EnableScaling)
	scheduler.SetScalingConfiguration(ScalingConfiguration{
		ScalingCheckIntervalInSeconds:      1,
		MaxConcurrencyPerReplica:           max(1, int64(opts.Worker)),
		Replica:                            max(1, int64(opts.Replica)),
		ArrivalRateThresholdPerReplica:     1,
		AllocatedBudgetViolationThreshold:  1,
		CumulativeBudgetViolationThreshold: 1,
	})

	if opts.EnableScaling {
		// create connection
		log.Info("Enabled ScalingMetric Reporting For Scheduler")
		conn, err := CreateGrpcConnection(ctx, opts.ScalingServerAddress, opts.ScalingServerPort)
		if err != nil {
			log.Error("Failed to connect to Scaling Server Running at %s:%d with error : %s", opts.ScalingServerAddress, opts.ScalingServerPort, err.Error())
		}

		scheduler.SetScalingMetricsMonitoring(NewScalingMetricsMonitoring(1000, conn))
	}

	scheduler.UpdateWorkers(int64(opts.Worker * opts.Replica))
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
