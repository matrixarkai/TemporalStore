package temporalstore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type ProxyOptions struct {
	Endpoint      string
	NamespaceName string
	TableName     string
	APIKey        string
	Timeout       time.Duration
}

type ProxyClient struct {
	endpoint string
	options  ProxyOptions
	client   *http.Client
}

func ConnectProxy(options ProxyOptions) *ProxyClient {
	timeout := options.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	return &ProxyClient{
		endpoint: strings.TrimRight(options.Endpoint, "/"),
		options:  options,
		client:   &http.Client{Timeout: timeout},
	}
}

func (c *ProxyClient) PutString(key, value string, ttlMs uint64) error {
	body := c.keyBody(key)
	body["value"] = value
	if ttlMs > 0 {
		body["ttl_ms"] = ttlMs
	}
	return c.post("/v1/string/put", body, nil)
}

func (c *ProxyClient) GetString(key string) (string, error) {
	var data struct {
		Value string `json:"value"`
	}
	err := c.post("/v1/string/get", c.keyBody(key), &data)
	return data.Value, err
}

func (c *ProxyClient) DeleteObject(key string) error {
	return c.post("/v1/common/delete", c.keyBody(key), nil)
}

func (c *ProxyClient) Expire(key string, ttlMs uint64) error {
	body := c.keyBody(key)
	body["ttl_ms"] = ttlMs
	return c.post("/v1/common/expire", body, nil)
}

func (c *ProxyClient) TTL(key string) (uint64, error) {
	var data struct {
		TTL uint64 `json:"ttl_ms"`
	}
	err := c.post("/v1/common/ttl", c.keyBody(key), &data)
	return data.TTL, err
}

func (c *ProxyClient) HSet(key, field, value string) error {
	body := c.keyBody(key)
	body["field"] = field
	body["value"] = value
	return c.post("/v1/hash/hset", body, nil)
}

func (c *ProxyClient) HGet(key, field string) (string, error) {
	body := c.keyBody(key)
	body["field"] = field
	var data struct {
		Value string `json:"value"`
	}
	err := c.post("/v1/hash/hget", body, &data)
	return data.Value, err
}

func (c *ProxyClient) HDel(key, field string) error {
	body := c.keyBody(key)
	body["field"] = field
	return c.post("/v1/hash/hdel", body, nil)
}

func (c *ProxyClient) SAdd(key, member string) error {
	body := c.keyBody(key)
	body["member"] = member
	return c.post("/v1/set/sadd", body, nil)
}

func (c *ProxyClient) SMembers(key string) ([]string, error) {
	var data struct {
		Members []string `json:"members"`
	}
	err := c.post("/v1/set/smembers", c.keyBody(key), &data)
	return data.Members, err
}

func (c *ProxyClient) AddFeaturePoints(key string, points []FeaturePoint) error {
	body := c.keyBody(key)
	body["points"] = points
	return c.post("/v1/feature/add", body, nil)
}

func (c *ProxyClient) QueryFeaturePoints(
	key string,
	startTs uint64,
	endTs uint64,
	count uint64,
	filters []FeatureFilter,
) ([]FeaturePoint, error) {
	body := c.queryBody(key, startTs, endTs, count, filters)
	var data struct {
		Points []FeaturePoint `json:"points"`
	}
	err := c.post("/v1/feature/query", body, &data)
	return data.Points, err
}

func (c *ProxyClient) AddSequenceFeatureRows(key string, rows []SequenceFeatureRow) error {
	body := c.keyBody(key)
	body["rows"] = rows
	return c.post("/v1/sequence/add", body, nil)
}

func (c *ProxyClient) QuerySequenceFeatureRows(
	key string,
	startTs uint64,
	endTs uint64,
	count uint64,
	filters []FeatureFilter,
) ([]SequenceFeatureRow, error) {
	body := c.queryBody(key, startTs, endTs, count, filters)
	var data struct {
		Rows []SequenceFeatureRow `json:"rows"`
	}
	err := c.post("/v1/sequence/query", body, &data)
	return data.Rows, err
}

func (c *ProxyClient) AddIPSInstance(
	ipsTable string,
	uid int64,
	timestampUs int64,
	actionType int32,
	logicalTable int32,
	features []IpsFeatureStat,
) error {
	body := map[string]interface{}{
		"namespace":     c.options.NamespaceName,
		"table":         c.options.TableName,
		"ips_table":     ipsTable,
		"uid":           uid,
		"timestamp_us":  timestampUs,
		"action_type":   actionType,
		"logical_table": logicalTable,
		"features":      features,
	}
	return c.post("/v1/ips/add", body, nil)
}

func (c *ProxyClient) QueryIPSLastInstances(
	ipsTable string,
	uid int64,
	actionType int32,
	logicalTable int32,
	slot int32,
	topK int32,
	lastInstances int64,
) ([]IpsFeatureStat, error) {
	body := map[string]interface{}{
		"namespace":      c.options.NamespaceName,
		"table":          c.options.TableName,
		"ips_table":      ipsTable,
		"uid":            uid,
		"action_type":    actionType,
		"logical_table":  logicalTable,
		"slot":           slot,
		"top_k":          topK,
		"last_instances": lastInstances,
	}
	var data struct {
		Features []IpsFeatureStat `json:"features"`
	}
	err := c.post("/v1/ips/query_last", body, &data)
	return data.Features, err
}

func (c *ProxyClient) RiskIncrement(
	key string,
	amount int64,
	ttlSeconds uint64,
	precision RiskPrecision,
	uuid string,
	occurTimeSeconds uint64,
) error {
	body := c.keyBody(key)
	body["amount"] = amount
	body["ttl_seconds"] = ttlSeconds
	body["precision"] = precision
	body["uuid"] = uuid
	body["occur_time_seconds"] = occurTimeSeconds
	return c.post("/v1/risk/increment", body, nil)
}

func (c *ProxyClient) RiskCount(
	key string,
	precision RiskPrecision,
	windowStart int64,
	windowEnd int64,
	windowUnit WindowUnit,
) (int64, error) {
	body := c.keyBody(key)
	body["precision"] = precision
	body["window_start"] = windowStart
	body["window_end"] = windowEnd
	body["window_unit"] = windowUnit
	var data struct {
		Count int64 `json:"count"`
	}
	err := c.post("/v1/risk/count", body, &data)
	return data.Count, err
}

func (c *ProxyClient) keyBody(key string) map[string]interface{} {
	return map[string]interface{}{
		"namespace": c.options.NamespaceName,
		"table":     c.options.TableName,
		"key":       key,
	}
}

func (c *ProxyClient) queryBody(
	key string,
	startTs uint64,
	endTs uint64,
	count uint64,
	filters []FeatureFilter,
) map[string]interface{} {
	body := c.keyBody(key)
	body["start_ts"] = startTs
	body["end_ts"] = endTs
	body["count"] = count
	body["filters"] = filters
	return body
}

func (c *ProxyClient) post(path string, body interface{}, out interface{}) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, c.endpoint+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.options.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.options.APIKey)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var envelope struct {
		OK      bool            `json:"ok"`
		Code    int             `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("temporalstore proxy http %d: %s", resp.StatusCode, envelope.Message)
	}
	if !envelope.OK {
		return fmt.Errorf("temporalstore proxy code %d: %s", envelope.Code, envelope.Message)
	}
	if out != nil && len(envelope.Data) > 0 {
		return json.Unmarshal(envelope.Data, out)
	}
	return nil
}
