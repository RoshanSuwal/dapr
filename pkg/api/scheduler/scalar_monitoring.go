package scheduler

import (
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"github.com/dapr/dapr/pkg/proto/common/v1"
	runtimev1pb "github.com/dapr/dapr/pkg/proto/runtime/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"
	"math"
	"net"
	"strconv"
	"time"
)

var (
	ScalingMetricReportEndpoint                   = "scalingMetricReport"
	MinDurationBetweenScalingReportInMilliSeconds = int64(1000)
)

type ScalingMetricsReportResponse struct {
	AppId                              string
	Timestamp                          int64
	MaxConcurrencyPerReplica           int64
	Replica                            int64
	ArrivalRateThresholdPerReplica     float64
	AllocatedBudgetViolationThreshold  float64
	CumulativeBudgetViolationThreshold float64
	ScalingCheckIntervalInSeconds      int64
	ScalingMetricWindowPeriodInSeconds int
	ScalingMetricListMaxSize           int
}

type ScalingMetricsReportRequest struct {
	AppId                          string
	Timestamp                      int64
	AllocatedBudgetViolationRatio  float64
	CumulativeBudgetViolationRatio float64
	ArrivalRate                    int64
	MeanQueuingDelay               int64
}

type ScalingParamMetric struct {
	Timestamp                 int64
	Endpoint                  string
	AllocatedBudgetViolation  bool
	CumulativeBudgetViolation bool
	QueuingDelay              int64
}

// ScalingConfiguration pojo containing the scaling configuration
type ScalingConfiguration struct {
	ScalingCheckIntervalInSeconds      int64
	MaxConcurrencyPerReplica           int64
	Replica                            int64
	ArrivalRateThresholdPerReplica     float64
	AllocatedBudgetViolationThreshold  float64
	CumulativeBudgetViolationThreshold float64
}

type ScalingMetricsMonitoring struct {
	appId                               string
	maxSize                             int
	metricList                          *list.List
	allocatedBudgetViolationCount       int
	cumulativeBudgetViolationCount      int
	queuingDelaySum                     int64
	appCallbackClient                   runtimev1pb.AppCallbackClient
	arrivalRateCounter                  int
	cumulativeArrivalRateInWindowPeriod int        // cumulative arrival rate in  window
	arrivalRatePerSecondList            *list.List // contains the last N seconds arrival rate
	windowPeriodInSec                   int        // window period to calculate the arrival rate mean
	lastReportedTime                    int64
}

func (p *ScalingMetricsMonitoring) SetAppId(appId string) {
	p.appId = appId
}
func (p *ScalingMetricsMonitoring) SetMaxSize(size int) {
	p.maxSize = size
}

func (p *ScalingMetricsMonitoring) SetWindowPeriodInSec(windowPeriod int) {
	p.windowPeriodInSec = windowPeriod
}

// this function is triggered on every seconds
func (p *ScalingMetricsMonitoring) computeNewArrivalRate() {
	log.Debug("Computing the New Arrival Rate")
	p.arrivalRatePerSecondList.PushFront(p.arrivalRateCounter)
	p.cumulativeArrivalRateInWindowPeriod += p.arrivalRateCounter
	p.arrivalRateCounter = 0
	for p.arrivalRatePerSecondList.Len() > p.windowPeriodInSec {
		p.cumulativeArrivalRateInWindowPeriod -= max(p.arrivalRatePerSecondList.Remove(p.arrivalRatePerSecondList.Back()).(int), 0)
	}
}

func (p *ScalingMetricsMonitoring) checkBudgetViolation(sc ScalingConfiguration) bool {
	if p.metricList.Len() < (p.maxSize / 10) {
		return false
	}
	return float64(p.allocatedBudgetViolationCount/p.metricList.Len()) > sc.AllocatedBudgetViolationThreshold
}

func (p *ScalingMetricsMonitoring) addMetric(endpoint string, cumulativeBudget int64, allocatedBudget int64, queuingDelay int64, requestTimestamp int64) {
	metric := p.metricList.PushBack(&ScalingParamMetric{
		Endpoint:                  endpoint,
		CumulativeBudgetViolation: cumulativeBudget < queuingDelay,
		AllocatedBudgetViolation:  allocatedBudget < queuingDelay,
		QueuingDelay:              queuingDelay,
		Timestamp:                 requestTimestamp,
	}).Value.(*ScalingParamMetric)

	p.arrivalRateCounter++
	p.metricList.PushFront(metric)
	p.queuingDelaySum += metric.QueuingDelay

	if metric.CumulativeBudgetViolation {
		p.cumulativeBudgetViolationCount++
	}
	if metric.AllocatedBudgetViolation {
		p.allocatedBudgetViolationCount++
	}

	for p.metricList.Len() > p.maxSize {
		rm := p.metricList.Remove(p.metricList.Back()).(*ScalingParamMetric)
		p.queuingDelaySum -= rm.QueuingDelay
		if rm.CumulativeBudgetViolation {
			p.cumulativeBudgetViolationCount--
		}
		if rm.AllocatedBudgetViolation {
			p.allocatedBudgetViolationCount--
		}
	}
}

func (p *ScalingMetricsMonitoring) GetMetricReport() *ScalingMetricsReportRequest {
	return &ScalingMetricsReportRequest{
		AppId:                          p.appId,
		AllocatedBudgetViolationRatio:  float64(p.allocatedBudgetViolationCount / max(p.metricList.Len(), p.maxSize/10)),
		CumulativeBudgetViolationRatio: float64(p.cumulativeBudgetViolationCount / max(p.metricList.Len(), p.maxSize/10)),
		MeanQueuingDelay:               p.queuingDelaySum / int64(max(p.metricList.Len(), p.maxSize/10)),
		ArrivalRate:                    int64(math.Ceil(float64(p.cumulativeArrivalRateInWindowPeriod / p.windowPeriodInSec))),
	}
}

func (p *ScalingMetricsMonitoring) SendReportToScalar(ctx context.Context) (scalingMetricReportResponse *ScalingMetricsReportResponse, err error) {
	if time.Now().UnixMilli()-p.lastReportedTime < MinDurationBetweenScalingReportInMilliSeconds {
		return nil, fmt.Errorf("too frequent sending scaling metrics report. Atleast %d  milli seconds duration between each reporting", MinDurationBetweenScalingReportInMilliSeconds)
	}

	content, _ := json.Marshal(p.GetMetricReport())
	invokeResponse, err := p.appCallbackClient.OnInvoke(ctx, &common.InvokeRequest{
		Method:      ScalingMetricReportEndpoint,
		Data:        &anypb.Any{Value: content},
		ContentType: "application/json",
	})
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(invokeResponse.Data.Value, &scalingMetricReportResponse); err != nil {
		return nil, err
	}
	return
}

func NewScalingMetricsMonitoring(maxSize int, conn *grpc.ClientConn) *ScalingMetricsMonitoring {
	return &ScalingMetricsMonitoring{
		maxSize:                  maxSize,
		metricList:               list.New(),
		arrivalRatePerSecondList: list.New(),
		appCallbackClient:        runtimev1pb.NewAppCallbackClient(conn),
		windowPeriodInSec:        10,
	}
}

func CreateGrpcConnection(ctx context.Context, host string, port int) (*grpc.ClientConn, error) {
	address := net.JoinHostPort(host, strconv.Itoa(port))
	return grpc.DialContext(ctx, address, grpc.WithInsecure())
}
