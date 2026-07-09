package temporalstore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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
	body["value"] = bytesValue(value)
	if ttlMs > 0 {
		body["ttl_ms"] = ttlMs
		return c.post("/ProxyService/SetEx", body, nil)
	}
	return c.post("/ProxyService/Set", body, nil)
}

func (c *ProxyClient) GetString(key string) (string, error) {
	var data struct {
		Value string `json:"value"`
	}
	err := c.post("/ProxyService/Get", c.keyBody(key), &data)
	return data.Value, err
}

func (c *ProxyClient) DeleteObject(key string) error {
	return c.post("/ProxyService/Delete", c.keyBody(key), nil)
}

func (c *ProxyClient) Expire(key string, ttlMs uint64) error {
	body := c.keyBody(key)
	body["ttl_ms"] = ttlMs
	return c.post("/ProxyService/Expire", body, nil)
}

func (c *ProxyClient) TTL(key string) (uint64, error) {
	var data struct {
		TTL uint64 `json:"ttl_ms"`
	}
	err := c.post("/ProxyService/Ttl", c.keyBody(key), &data)
	return data.TTL, err
}

func (c *ProxyClient) HSet(key, field, value string) error {
	body := c.keyBody(key)
	body["field"] = field
	body["value"] = bytesValue(value)
	return c.post("/ProxyService/HSet", body, nil)
}

func (c *ProxyClient) HGet(key, field string) (string, error) {
	body := c.keyBody(key)
	body["field"] = field
	var data struct {
		Value string `json:"value"`
	}
	err := c.post("/ProxyService/HGet", body, &data)
	return data.Value, err
}

func (c *ProxyClient) HDel(key, field string) error {
	body := c.keyBody(key)
	body["field"] = field
	return c.post("/ProxyService/HDel", body, nil)
}

func (c *ProxyClient) SAdd(key, member string) error {
	body := c.keyBody(key)
	body["member"] = bytesValue(member)
	return c.post("/ProxyService/SAdd", body, nil)
}

func (c *ProxyClient) SMembers(key string) ([]string, error) {
	var data struct {
		Members []string `json:"members"`
	}
	err := c.post("/ProxyService/SMembers", c.keyBody(key), &data)
	return data.Members, err
}

func (c *ProxyClient) AddFeaturePoints(key string, points []FeaturePoint) error {
	body := c.keyBody(key)
	body["points"] = featurePointBodies(points)
	return c.post("/ProxyService/FeatureAdd", body, nil)
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
	err := c.post("/ProxyService/FeatureQuery", body, &data)
	return data.Points, err
}

func (c *ProxyClient) AddSequenceFeatureRows(key string, rows []SequenceFeatureRow) error {
	body := c.keyBody(key)
	body["rows"] = sequenceRowBodies(rows)
	return c.post("/ProxyService/SequenceAdd", body, nil)
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
	err := c.post("/ProxyService/SequenceQuery", body, &data)
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
		"table_name":    c.options.TableName,
		"ips_table":     ipsTable,
		"key":           ipsTable,
		"uid":           uid,
		"timestamp_us":  timestampUs,
		"timestamp_ms":  timestampUs / 1000,
		"action_type":   actionType,
		"table_id":      logicalTable,
		"logical_table": logicalTable,
		"features":      features,
	}
	instance, err := json.Marshal(features)
	if err != nil {
		return err
	}
	body["instance"] = bytesValue(string(instance))
	return c.post("/ProxyService/IpsAdd", body, nil)
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
		"table_name":     c.options.TableName,
		"ips_table":      ipsTable,
		"key":            ipsTable,
		"uid":            uid,
		"action_type":    actionType,
		"logical_table":  logicalTable,
		"slot":           slot,
		"top_k":          topK,
		"last_instances": lastInstances,
		"count":          lastInstances,
	}
	var data struct {
		Features []IpsFeatureStat `json:"features"`
	}
	err := c.post("/ProxyService/IpsQueryLast", body, &data)
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
	body["ttl_ms"] = ttlSeconds * 1000
	body["precision"] = precision
	body["precision_ms"] = riskPrecisionMs(precision)
	body["uuid"] = uuid
	body["occur_time_seconds"] = occurTimeSeconds
	body["timestamp_ms"] = proxyTimestampMs(occurTimeSeconds)
	return c.post("/ProxyService/RiskIncrement", body, nil)
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
	startMs, endMs := riskWindowMs(windowStart, windowEnd, windowUnit)
	body["start_ms"] = startMs
	body["end_ms"] = endMs
	var data struct {
		Count int64 `json:"count"`
	}
	err := c.post("/ProxyService/RiskCount", body, &data)
	return data.Count, err
}

func (c *ProxyClient) keyBody(key string) map[string]interface{} {
	return map[string]interface{}{
		"namespace":  c.options.NamespaceName,
		"table":      c.options.TableName,
		"table_name": c.options.TableName,
		"key":        key,
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
	body["start_ms"] = startTs
	body["end_ms"] = endTs
	body["count"] = count
	body["filters"] = featureFilterBodies(filters)
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

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("temporalstore proxy http %d: %s", resp.StatusCode, string(responseBody))
	}
	if len(responseBody) == 0 {
		return nil
	}
	data, err := unwrapProxyResponse(responseBody)
	if err != nil {
		return err
	}
	if out == nil {
		return nil
	}
	if len(data) > 0 {
		return json.Unmarshal(data, out)
	}
	return nil
}

func unwrapProxyResponse(body []byte) (json.RawMessage, error) {
	var envelope struct {
		OK      *bool           `json:"ok"`
		Code    interface{}     `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err == nil && envelope.OK != nil {
		if !*envelope.OK {
			return nil, fmt.Errorf("temporalstore proxy code %v: %s", envelope.Code, envelope.Message)
		}
		return envelope.Data, nil
	}

	var executed struct {
		Status struct {
			OK      bool   `json:"ok"`
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"status"`
		Response json.RawMessage `json:"response"`
	}
	if err := json.Unmarshal(body, &executed); err == nil && len(executed.Response) > 0 {
		if !executed.Status.OK {
			return nil, fmt.Errorf("temporalstore proxy code %s: %s", executed.Status.Code, executed.Status.Message)
		}
		return flattenCommandResponse(executed.Response)
	}

	return body, nil
}

func flattenCommandResponse(raw json.RawMessage) (json.RawMessage, error) {
	var response map[string]json.RawMessage
	if err := json.Unmarshal(raw, &response); err != nil {
		return nil, err
	}
	var kind string
	_ = json.Unmarshal(response["kind"], &kind)
	switch kind {
	case "empty":
		return []byte("{}"), nil
	case "bytes":
		var value []int
		if len(response["value"]) > 0 && !bytes.Equal(response["value"], []byte("null")) {
			_ = json.Unmarshal(response["value"], &value)
		}
		payload := map[string]interface{}{"value": intsToString(value)}
		return json.Marshal(payload)
	case "integer", "aggregate":
		var value int64
		_ = json.Unmarshal(response["value"], &value)
		payload := map[string]interface{}{"value": value, "count": value, "ttl_ms": uint64(value)}
		return json.Marshal(payload)
	case "members":
		var members [][]int
		_ = json.Unmarshal(response["members"], &members)
		values := make([]string, 0, len(members))
		for _, member := range members {
			values = append(values, intsToString(member))
		}
		return json.Marshal(map[string]interface{}{"members": values})
	case "feature_points":
		return flattenFeaturePoints(response["points"])
	case "sequence_rows":
		return flattenSequenceRows(response["rows"])
	default:
		return raw, nil
	}
}

func featurePointBodies(points []FeaturePoint) []map[string]interface{} {
	out := make([]map[string]interface{}, 0, len(points))
	for _, point := range points {
		out = append(out, map[string]interface{}{
			"timestamp":    point.Timestamp,
			"timestamp_ms": point.Timestamp,
			"value":        bytesValue(point.Value),
		})
	}
	return out
}

func sequenceRowBodies(rows []SequenceFeatureRow) []map[string]interface{} {
	out := make([]map[string]interface{}, 0, len(rows))
	for _, row := range rows {
		out = append(out, map[string]interface{}{
			"timestamp":    row.Timestamp,
			"timestamp_ms": row.Timestamp,
			"gid":          row.GID,
			"action_type":  row.ActionType,
			"duration":     row.Duration,
			"author_id":    row.AuthorID,
		})
	}
	return out
}

func featureFilterBodies(filters []FeatureFilter) []map[string]interface{} {
	out := make([]map[string]interface{}, 0, len(filters))
	for _, filter := range filters {
		out = append(out, map[string]interface{}{
			"field": filter.Field,
			"op":    featureFilterOpName(filter.Op),
			"value": filter.Value,
		})
	}
	return out
}

func featureFilterOpName(op FeatureFilterOp) string {
	switch op {
	case FeatureFilterNotEqual:
		return "not_equal"
	case FeatureFilterGreaterThan:
		return "greater_than"
	case FeatureFilterLessThan:
		return "less_than"
	default:
		return "equal"
	}
}

func flattenFeaturePoints(raw json.RawMessage) (json.RawMessage, error) {
	var points []map[string]json.RawMessage
	_ = json.Unmarshal(raw, &points)
	outPoints := make([]map[string]interface{}, 0, len(points))
	features := make([]IpsFeatureStat, 0)
	for _, point := range points {
		var timestamp uint64
		_ = json.Unmarshal(point["timestamp_ms"], &timestamp)
		var value []int
		_ = json.Unmarshal(point["value"], &value)
		text := intsToString(value)
		outPoints = append(outPoints, map[string]interface{}{"timestamp": timestamp, "timestamp_ms": timestamp, "value": text})
		var decoded []IpsFeatureStat
		if err := json.Unmarshal([]byte(text), &decoded); err == nil {
			features = append(features, decoded...)
		}
	}
	return json.Marshal(map[string]interface{}{"points": outPoints, "features": features})
}

func flattenSequenceRows(raw json.RawMessage) (json.RawMessage, error) {
	var rows []map[string]interface{}
	_ = json.Unmarshal(raw, &rows)
	for _, row := range rows {
		if timestamp, ok := row["timestamp_ms"]; ok {
			row["timestamp"] = timestamp
		}
	}
	return json.Marshal(map[string]interface{}{"rows": rows})
}

func proxyTimestampMs(occurTimeSeconds uint64) uint64 {
	if occurTimeSeconds == 0 {
		return uint64(time.Now().UnixMilli())
	}
	return occurTimeSeconds * 1000
}

func riskPrecisionMs(precision RiskPrecision) uint64 {
	switch precision {
	case RiskOneSecond:
		return 1000
	case RiskFiveSeconds:
		return 5000
	case RiskTenSeconds:
		return 10000
	case RiskOneMinute:
		return 60000
	case RiskFiveMinutes:
		return 5 * 60000
	case RiskTenMinutes:
		return 10 * 60000
	case RiskOneHour:
		return 60 * 60000
	case RiskOneDay:
		return 24 * 60 * 60000
	case RiskOneMonth:
		return 30 * 24 * 60 * 60000
	default:
		return 60000
	}
}

func riskWindowMs(windowStart int64, windowEnd int64, unit WindowUnit) (uint64, uint64) {
	now := time.Now().UnixMilli()
	end := windowEnd
	if end <= 0 {
		end = now
	}
	start := windowStart
	if start < 0 {
		start = end - int64(windowUnitMs(unit))
	}
	if start < 0 {
		start = 0
	}
	return uint64(start), uint64(end)
}

func windowUnitMs(unit WindowUnit) uint64 {
	switch unit {
	case WindowSecond:
		return 1000
	case WindowMinute:
		return 60000
	case WindowDay:
		return 24 * 60 * 60000
	default:
		return 60 * 60000
	}
}

func bytesValue(value string) []int {
	out := make([]int, 0, len(value))
	for _, b := range []byte(value) {
		out = append(out, int(b))
	}
	return out
}

func intsToString(value []int) string {
	out := make([]byte, 0, len(value))
	for _, b := range value {
		out = append(out, byte(b))
	}
	return string(out)
}
