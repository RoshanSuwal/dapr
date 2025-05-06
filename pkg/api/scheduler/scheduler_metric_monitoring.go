package scheduler

import (
	"context"
	diagUtils "github.com/dapr/dapr/pkg/diagnostics/utils"
	"github.com/prometheus/client_golang/prometheus"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

// SchedulerMetrics Scheduler Monitoring
var (
	appIDKey                           = tag.MustNewKey("app_id")
	KeyMethod                          = tag.MustNewKey("method")
	KeyEndpoint                        = tag.MustNewKey("endpoint")
	KeyBudgetViolationStatus           = tag.MustNewKey("budget_violation_status")
	KeyCumulativeBudgetViolationStatus = tag.MustNewKey("cumulative_budget_violation_status")
)

type schedulerMetricsMonitoring struct {
	appId                         string
	enabled                       bool
	queuingDelay                  *stats.Float64Measure
	serviceTime                   *stats.Float64Measure
	responseTime                  *stats.Float64Measure
	queueSize                     *stats.Int64Measure
	budget                        *stats.Float64Measure
	serviceTimeResponseTimeRatio  *stats.Float64Measure // service_time / response_time
	queuingDelayResponseTimeRatio *stats.Float64Measure // queuing_delay / response_time
	cumulativeBudget              *stats.Float64Measure
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
		queuingDelayResponseTimeRatio: stats.Float64(
			"scheduler/queuing_delay_response_time_ratio",
			"Budget violation status",
			stats.UnitDimensionless),
		cumulativeBudget: stats.Float64(
			"scheduler/cumulative_budget",
			"cumulative budget per request",
			stats.UnitMilliseconds),
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
		diagUtils.NewMeasureView(m.budget, []tag.Key{appIDKey, KeyMethod, KeyEndpoint, KeyBudgetViolationStatus, KeyCumulativeBudgetViolationStatus}, latencyDistribution),
		diagUtils.NewMeasureView(m.cumulativeBudget, []tag.Key{appIDKey, KeyMethod, KeyEndpoint, KeyBudgetViolationStatus, KeyCumulativeBudgetViolationStatus}, latencyDistribution),
		diagUtils.NewMeasureView(m.queueSize, []tag.Key{appIDKey, KeyMethod, KeyEndpoint, KeyBudgetViolationStatus}, latencyDistribution),
		diagUtils.NewMeasureView(m.serviceTimeResponseTimeRatio, []tag.Key{appIDKey, KeyMethod, KeyEndpoint, KeyBudgetViolationStatus}, view.Distribution(prometheus.LinearBuckets(0.0, 0.05, 21)...)),
		diagUtils.NewMeasureView(m.queuingDelayResponseTimeRatio, []tag.Key{appIDKey, KeyMethod, KeyEndpoint, KeyBudgetViolationStatus}, view.Distribution(prometheus.LinearBuckets(0.0, 0.05, 21)...)),
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
		float64(r.CumulativeBudget/1000),
	)
}

// MonitorRequest All time unit must be in milliseconds
func (m *schedulerMetricsMonitoring) MonitorRequest(ctx context.Context, method string, endpoint string, queuingDelay float64, serviceTime float64, responseTime float64, queueSize int64, budget float64, cumulativeBudget float64) {
	if !m.IsEnabled() {
		return
	}

	budgetViolationStatus := "false"
	cumulativeBudgetViolationStatus := "false"

	if budget > 0 && budget-queuingDelay < 0 {
		budgetViolationStatus = "true"
	}

	if cumulativeBudget-queuingDelay < 0 {
		cumulativeBudgetViolationStatus = "true"
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
		diagUtils.WithTags(m.budget.Name(), appIDKey, m.appId, KeyMethod, method, KeyEndpoint, endpoint, KeyBudgetViolationStatus, budgetViolationStatus, KeyCumulativeBudgetViolationStatus, cumulativeBudgetViolationStatus),
		m.budget.M(budget))

	stats.RecordWithTags(ctx,
		diagUtils.WithTags(m.cumulativeBudget.Name(), appIDKey, m.appId, KeyMethod, method, KeyEndpoint, endpoint, KeyBudgetViolationStatus, budgetViolationStatus, KeyCumulativeBudgetViolationStatus, cumulativeBudgetViolationStatus),
		m.cumulativeBudget.M(cumulativeBudget))

	stats.RecordWithTags(ctx,
		diagUtils.WithTags(m.responseTime.Name(), appIDKey, m.appId, KeyMethod, method, KeyEndpoint, endpoint, KeyBudgetViolationStatus, budgetViolationStatus),
		m.responseTime.M(responseTime))

	stats.RecordWithTags(ctx,
		diagUtils.WithTags(m.serviceTimeResponseTimeRatio.Name(), appIDKey, m.appId, KeyMethod, method, KeyEndpoint, endpoint, KeyBudgetViolationStatus, budgetViolationStatus),
		m.serviceTimeResponseTimeRatio.M(serviceTime/responseTime))

	stats.RecordWithTags(ctx,
		diagUtils.WithTags(m.queuingDelayResponseTimeRatio.Name(), appIDKey, m.appId, KeyMethod, method, KeyEndpoint, endpoint, KeyBudgetViolationStatus, budgetViolationStatus),
		m.serviceTimeResponseTimeRatio.M(queuingDelay/responseTime))
}
