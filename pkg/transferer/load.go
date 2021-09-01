package transferer

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/valyala/fasthttp"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type LoadStatus int

const (
	LoadStatusNone LoadStatus = iota
	LoadStatusOK
	LoadStatusWait
	LoadStatusTerminate

	MaxWaitStatusInSequence int = 10
)

func (s LoadStatus) String() string {
	switch s {
	case LoadStatusNone:
		return "NONE"
	case LoadStatusOK:
		return "OK"
	case LoadStatusWait:
		return "WAIT"
	case LoadStatusTerminate:
		return "TERMINATE"
	default:
		return "UNDEFINED"
	}
}

const (
	MaxLoadWaitDuration = time.Second
)

type LoadChecker struct {
	c             *fasthttp.Client
	connectionURL string

	thresholds []Threshold

	m            sync.RWMutex
	latestStatus LoadStatus

	waitStatusCounter int
}

func NewLoadChecker(ctx context.Context, c *fasthttp.Client, url, maxLoad, criticalLoad string) (*LoadChecker, error) {
	thresholds, err := parseThresholds(maxLoad, criticalLoad)
	if err != nil {
		return nil, err
	}

	lc := &LoadChecker{
		c:             c,
		connectionURL: url,
		thresholds:    thresholds,
		latestStatus:  LoadStatusWait,
	}

	lc.updateStatus()

	lc.runStatusUpdate(ctx)
	return lc, nil
}

func (c *LoadChecker) GetLatestStatus() LoadStatus {
	c.m.RLock()
	defer c.m.RUnlock()
	return c.latestStatus
}

func (c *LoadChecker) setLatestStatus(s LoadStatus) {
	c.m.Lock()
	defer c.m.Unlock()
	c.latestStatus = s
}

func (c *LoadChecker) runStatusUpdate(ctx context.Context) {
	go func() {
		log.Debug().Msg("Started load status update")
		ticker := time.NewTicker(MaxLoadWaitDuration)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				log.Debug().Msgf("Context is done: stopping load status update")
				return
			case <-ticker.C:
				c.updateStatus()
			}
		}
	}()
}

func (c *LoadChecker) updateStatus() {
	status, err := c.checkMetricsLoad()
	if err != nil {
		status = LoadStatusWait
		log.Warn().Err(err).Msgf("Error while checking metrics load")
	}
	if status == LoadStatusWait {
		c.waitStatusCounter++
		if c.waitStatusCounter > MaxWaitStatusInSequence {
			log.Debug().Msgf("Reached max %v status attempts. Sending %v status", LoadStatusWait, LoadStatusTerminate)
			status = LoadStatusTerminate
		}
	} else {
		c.waitStatusCounter = 0
	}

	c.setLatestStatus(status)
	log.Debug().Msgf("Load status now is %v", status)
}

func (c *LoadChecker) checkMetricsLoad() (LoadStatus, error) {
	log.Debug().Msg("Started check load status")
	loadStatus := LoadStatusOK
	for _, t := range c.thresholds {
		value, err := c.getMetricCurrentValue(t)
		if err != nil {
			return LoadStatusNone, fmt.Errorf("failed to retrieve threshold value for %s: %w", t.Key, err)
		}
		switch {
		case value >= t.CriticalLoad:
			log.Debug().Msgf("Checked %s threshold: it exceeds critical load limit. Terminating", t.Key)
			return LoadStatusTerminate, nil
		case value >= t.MaxLoad:
			log.Debug().Msgf("Checked %s threshold: it exceeds max load limit. Continue checking", t.Key)
			loadStatus = LoadStatusWait
		default:
			log.Debug().Msgf("Checked %s threshold: it's ok. Continue checking", t.Key)
		}
	}

	log.Debug().Msgf("Checked all thresholds: final status is %v", loadStatus)

	return loadStatus, nil
}

func (c *LoadChecker) getMetricCurrentValue(m Threshold) (float64, error) {
	q := fasthttp.AcquireArgs()
	defer fasthttp.ReleaseArgs(q)

	q.Add("query", m.Query)

	url := fmt.Sprintf("%s/api/v1/query?%s", c.connectionURL, q.String())

	log.Debug().
		Str("url", url).
		Msgf("Sending HTTP request to load checker endpoint")
	status, body, err := c.c.Get(nil, url)
	if err != nil {
		return 0, errors.Wrap(err, "failed to send req to load checker endpoint")
	}
	if status != http.StatusOK {
		return 0, fmt.Errorf("non-ok response: status %d: %s", status, string(body))
	}
	log.Debug().Msg("Got HTTP status OK from load checker endpoint")

	var resp metricResponse

	if err = json.Unmarshal(body, &resp); err != nil {
		return 0, fmt.Errorf("error parsing thresholds: %s", err)
	}

	value, err := resp.getValidValue()
	if err != nil {
		return 0, fmt.Errorf("error parsing threshold: %s", err)
	}
	log.Debug().Msgf("Got %f threshold value", value)
	return value, nil
}

type Threshold struct {
	Key          string
	Query        string
	MaxLoad      float64
	CriticalLoad float64
}

type metricResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric struct {
				Instance string `json:"instance"`
			} `json:"metric"`
			Value []interface{} `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

func (r *metricResponse) getValidValue() (float64, error) {
	if r.Status != "success" {
		return 0, errors.New("status is not success")
	}
	if len(r.Data.Result) == 0 {
		return 0, errors.New("empty result")
	}
	if len(r.Data.Result[0].Value) != 2 {
		return 0, errors.New("unexpected number of values")
	}
	str, ok := r.Data.Result[0].Value[1].(string)
	if !ok {
		return 0, errors.New("value is not string")
	}
	val, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing value error: %s", err.Error())
	}
	return val, nil
}

func parseThresholds(maxLoad, criticalLoad string) ([]Threshold, error) {
	maxLoadMap, err := parseThreshold(maxLoad)
	if err != nil {
		return nil, err
	}
	criticalLoadMap, err := parseThreshold(criticalLoad)
	if err != nil {
		return nil, err
	}
	var result []Threshold
	for k, criticalLoadValue := range criticalLoadMap {
		maxLoadVal, _ := maxLoadMap[k]
		t, err := createThreshold(k, maxLoadVal, criticalLoadValue)
		if err != nil {
			return nil, err
		}
		result = append(result, t)
	}
	for k, maxLoadValue := range maxLoadMap {
		criticalLoadVal, ok := criticalLoadMap[k]
		if ok {
			continue
		}
		t, err := createThreshold(k, maxLoadValue, criticalLoadVal)
		if err != nil {
			return nil, err
		}
		result = append(result, t)
	}
	return result, nil
}

func parseThreshold(thresholdArr string) (th map[string]float64, err error) {
	th = make(map[string]float64)
	for _, threshold := range strings.Split(thresholdArr, ",") {
		if threshold == "" {
			continue
		}
		key, val, err := splitThreshold(threshold)
		if err != nil {
			return nil, err
		}
		th[key] = val
	}
	return
}

func splitThreshold(th string) (key string, val float64, err error) {
	var sep string
	if strings.Contains(th, ":") {
		if strings.Contains(th, "=") {
			return "", 0, errors.New("invalid threshold syntax: ':' and '=' separators in one threshold statement")
		}
		sep = ":"
	} else if strings.Contains(th, "=") {
		sep = "="
	}
	if sep == "" {
		return th, 0, nil
	}
	keyVal := strings.Split(th, sep)
	if len(keyVal) != 2 {
		return "", 0, fmt.Errorf("invalid threshold syntax: multiple '%s' separators in one threshold statement", sep)
	}
	loadValue, err := strconv.ParseFloat(keyVal[1], 64)
	if err != nil {
		return "", 0, err
	}
	return keyVal[0], loadValue, nil
}

func createThreshold(thresholdKey string, maxLoad, criticalLoad float64) (Threshold, error) {
	thresholdKey = strings.ToLower(thresholdKey)
	var query string
	var defaultMaxLoad, defaultCriticalLoad float64
	switch thresholdKey {
	case "memory":
		query = `100 * (1 - ((avg_over_time(node_memory_MemFree_bytes{node_name="pmm-server"}[5s]) + avg_over_time(node_memory_Cached_bytes{node_name="pmm-server"}[5s]) + avg_over_time(node_memory_Buffers_bytes{node_name="pmm-server"}[5s])) / avg_over_time(node_memory_MemTotal_bytes{node_name="pmm-server"}[5s])))`
		defaultMaxLoad = 50
		defaultCriticalLoad = 70
	case "cpu":
		query = `100 - (avg by (instance) (rate(node_cpu_seconds_total{mode="idle",node_name="pmm-server"}[5s])) * 100)`
		defaultMaxLoad = 50
		defaultCriticalLoad = 70
	default:
		return Threshold{}, fmt.Errorf("unknown threshold key: %s", thresholdKey)
	}
	if maxLoad == 0 {
		maxLoad = defaultMaxLoad
	}
	if criticalLoad == 0 {
		criticalLoad = defaultCriticalLoad
	}
	return Threshold{
		Key:          thresholdKey,
		Query:        query,
		MaxLoad:      maxLoad,
		CriticalLoad: criticalLoad,
	}, nil
}
