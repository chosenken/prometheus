package kairosdb

import (
	"math"
	"time"

	"github.com/ajityagaty/go-kairosdb/builder"
	"github.com/ajityagaty/go-kairosdb/client"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
)

// Client is the KairosDB Remote Storage Adapter
type Client struct {
	logger log.Logger

	client         client.Client
	ignoredSamples prometheus.Counter
}

// NewClient creates a new Client.
func NewClient(logger log.Logger, apiURL string) *Client {
	c := client.NewHttpClient(apiURL)
	if logger == nil {
		logger = log.NewNopLogger()
	}
	return &Client{
		logger: logger,
		client: c,
		ignoredSamples: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "prometheus_kairosdb_ignored_samples_total",
				Help: "The total number of samples not sent to KairosDB due to unsupported float values (Inf, -Inf, NaN).",
			},
		),
	}
}

// tagsFromMetric extracts KairosDB tags from a Prometheus metric.
func tagsFromMetric(m model.Metric) map[string]string {
	tags := make(map[string]string, len(m)-1)
	for l, v := range m {
		if l != model.MetricNameLabel {
			tags[string(l)] = string(v)
		}
	}
	return tags
}

// Write sends a batch of samples to KairosDB via its HTTP API.
func (c *Client) Write(samples model.Samples) error {
	mb := c.BuildMetrics(samples)
	resp, err := c.client.PushMetrics(mb)
	if resp != nil {
		level.Error(c.logger).Log("status code", resp.GetStatusCode())
		for _, e := range resp.GetErrors() {
			level.Error(c.logger).Log("err", e)
		}
	}

	return err
}

// BuildMetrics build the KairosDB Library Metric Builder which holds the metric in a KairosDB format.
func (c *Client) BuildMetrics(samples model.Samples) builder.MetricBuilder {
	mb := builder.NewMetricBuilder()
	for _, s := range samples {
		v := float64(s.Value)
		if math.IsNaN(v) || math.IsInf(v, 0) {
			level.Debug(c.logger).Log("msg", "cannot send  to KairosDB, skipping sample", "value", v, "sample", s)
			c.ignoredSamples.Inc()
			continue
		}
		metric := mb.AddMetric(string(s.Metric[model.MetricNameLabel]))
		// KairosDB timestamps are in milliseconds
		metric.AddDataPoint(s.Timestamp.UnixNano()/int64(time.Millisecond), v)
		tags := tagsFromMetric(s.Metric)
		for name, value := range tags {
			// KairosDB does not like tags with empty values
			if len(value) != 0 {
				metric.AddTag(name, value)
			}
		}
	}
	return mb
}

// Name identifies the client as a KairosDB Client.
func (c *Client) Name() string {
	return "kairosdb"
}

// Describe implements prometheus.Collector.
func (c *Client) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.ignoredSamples.Desc()
}

// Collect implements prometheus.Collector.
func (c *Client) Collect(ch chan<- prometheus.Metric) {
	ch <- c.ignoredSamples
}
