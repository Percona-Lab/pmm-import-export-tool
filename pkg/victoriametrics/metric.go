// Copyright 2023 Percona LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package victoriametrics

import (
	"encoding/json"
	"io"

	"github.com/pkg/errors"
)

type Metric struct {
	Metric     map[string]string `json:"metric"`
	Values     []float64         `json:"values"`
	Timestamps []int64           `json:"timestamps"`
}

func ParseMetrics(r io.Reader) ([]Metric, error) {
	var result []Metric
	decoder := json.NewDecoder(r)
	for {
		metric := Metric{}
		err := decoder.Decode(&metric)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, errors.Wrap(err, "failed to decode JSON stream")
		}
		result = append(result, metric)
	}
	return result, nil
}

type MetricResponse struct {
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
	Stats struct {
		SeriesFetched string `json:"seriesFetched"`
	} `json:"stats"`
}

func (r *MetricResponse) GetValidValue() (string, error) {
	if r.Status != "success" {
		return "", errors.New("status is not success")
	}
	if len(r.Data.Result) == 0 {
		return "", nil
	}
	if len(r.Data.Result[0].Value) != 2 { //nolint:mnd
		return "", errors.New("unexpected number of values")
	}
	str, ok := r.Data.Result[0].Value[1].(string)
	if !ok {
		return "", errors.New("value is not string")
	}
	return str, nil
}
