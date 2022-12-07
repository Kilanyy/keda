package scalers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/go-logr/logr"
	v2beta2 "k8s.io/api/autoscaling/v2beta2"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/metrics/pkg/apis/external_metrics"

	kedautil "github.com/kedacore/keda/v2/pkg/util"
)

type incortaTelemetryScaler struct {
	metricType v2beta2.MetricTargetType
	metadata   incortaTelemetryMetadata
	client     *http.Client
	logger     logr.Logger
}

type incortaTelemetryMetadata struct {
	clusterName string

	scalerIndex int
}

const (
	incortaTelemetryMetricType = "External"
)

// NewIncortaTelemetryScaler creates a new incortaTelemetryScaler
func NewIncortaTelemetryScaler(config *ScalerConfig) (Scaler, error) {
	metricType, err := GetMetricTargetType(config)
	if err != nil {
		return nil, fmt.Errorf("error getting scaler metric type: %s", err)
	}

	logger := InitializeLogger(config, "incorta_telemetry_scaler")

	incortaTelemetryMetadata, err := parseIncortaTelemetryMetadata(config, logger)
	if err != nil {
		return nil, fmt.Errorf("error parsing incortam telemetry metadata: %s", err)
	}

	client := getIncortaTelemetryClient(incortaTelemetryMetadata)
	return &incortaTelemetryScaler{
		client:     client,
		metricType: metricType,
		metadata:   incortaTelemetryMetadata,
		logger:     logger,
	}, nil
}

func parseIncortaTelemetryMetadata(config *ScalerConfig, logger logr.Logger) (incortaTelemetryMetadata, error) {
	meta := incortaTelemetryMetadata{}
	switch {
	case config.TriggerMetadata["clusterNameFromEnv"] != "":
		meta.clusterName = config.ResolvedEnv[config.TriggerMetadata["clusterNameFromEnv"]]
	case config.TriggerMetadata["clusterName"] != "":
		meta.clusterName = config.TriggerMetadata["clusterName"]
	default:
		return meta, errors.New("no clusterName given")
	}

	meta.scalerIndex = config.ScalerIndex
	return meta, nil
}

// IsActive determines if we need to scale from zero
func (s *incortaTelemetryScaler) IsActive(ctx context.Context) (bool, error) {
	isActive, err := s.checkTelemetryAPI()
	if err != nil {
		return false, err
	}

	return isActive, nil
}

func getIncortaTelemetryClient(metadata incortaTelemetryMetadata) *http.Client {
	client := &http.Client{}
	return client
}

// Close closes the incorta telemetry client
func (s *incortaTelemetryScaler) Close(context.Context) error {
	// underlying client will also be closed on admin's Close() call
	s.client.CloseIdleConnections()
	return nil
}

func (s *incortaTelemetryScaler) GetMetricSpecForScaling(context.Context) []v2beta2.MetricSpec {
	var metricName string
	metricName = fmt.Sprintf("incorta-telemetry-%s-tasks-api", s.metadata.clusterName)

	externalMetric := &v2beta2.ExternalMetricSource{
		Metric: v2beta2.MetricIdentifier{
			Name: GenerateMetricNameWithIndex(s.metadata.scalerIndex, kedautil.NormalizeString(metricName)),
		},
		Target: GetMetricTarget(s.metricType, 1),
	}
	metricSpec := v2beta2.MetricSpec{External: externalMetric, Type: incortaTelemetryMetricType}
	return []v2beta2.MetricSpec{metricSpec}
}

// GetMetrics returns value for a supported metric and an error if there is a problem getting the metric
func (s *incortaTelemetryScaler) GetMetrics(ctx context.Context, metricName string, metricSelector labels.Selector) ([]external_metrics.ExternalMetricValue, error) {
	isActive, err := s.checkTelemetryAPI()
	var bitSetVar = 0
	if isActive {
		bitSetVar = 1
	}
	if err != nil {
		return []external_metrics.ExternalMetricValue{}, err
	}
	metric := GenerateMetricInMili(metricName, float64(bitSetVar))

	return append([]external_metrics.ExternalMetricValue{}, metric), nil
}

func (s *incortaTelemetryScaler) checkTelemetryAPI() (bool, error) {
	var client = s.client
	var isActive bool = true
	var telemetryApiURL = fmt.Sprintf("http://%s-0.%s.ic-%s.svc.cluster.local:8080/incorta/bff/v1/telemetry/loader-status", s.metadata.clusterName, s.metadata.clusterName, s.metadata.clusterName)
	resp, err := client.Get(telemetryApiURL)
	if err != nil {
		return true, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	var jsonMap map[string]string
	json.Unmarshal([]byte(fmt.Sprintf("%s", body)), &jsonMap)
	json.MarshalIndent(jsonMap, "", "    ")
	if jsonMap["state"] == "IDLE" {
		isActive = false
	} else {
		isActive = true
	}
	return isActive, nil
}
