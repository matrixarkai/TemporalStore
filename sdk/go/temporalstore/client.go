//go:build temporalstore_direct

package temporalstore

/*
#cgo CFLAGS: -I${SRCDIR}/../../..
#include "src/client/temporalstore_c_client.h"
#include <stdlib.h>
*/
import "C"

import (
	"errors"
	"unsafe"
)

type Client struct {
	ptr *C.temporalstore_client_t
}

func Connect(options Options) (*Client, error) {
	var cOptions C.temporalstore_options_t
	C.temporalstore_options_init(&cOptions)

	owned := make([]*C.char, 0, 8)
	defer freeCStringList(owned)
	setString := func(dst **C.char, value string) {
		if value == "" {
			return
		}
		cValue := C.CString(value)
		owned = append(owned, cValue)
		*dst = cValue
	}

	setString(&cOptions.metaserver_addr, options.MetaserverAddr)
	setString(&cOptions.metaserver_consul, options.MetaserverConsul)
	setString(&cOptions.namespace_name, options.NamespaceName)
	setString(&cOptions.table_name, options.TableName)
	setString(&cOptions.idc, options.IDC)
	setString(&cOptions.host, options.Host)
	setString(&cOptions.psm, options.PSM)
	setString(&cOptions.log_dir, options.LogDir)
	if options.LogLevel != 0 {
		cOptions.log_level = C.int(options.LogLevel)
	}
	if options.IOTimeoutMs != 0 {
		cOptions.io_timeout_ms = C.int(options.IOTimeoutMs)
	}
	if options.ConnectTimeoutMs != 0 {
		cOptions.connect_timeout_ms = C.int(options.ConnectTimeoutMs)
	}
	if options.RequestTimeoutMs != 0 {
		cOptions.request_timeout_ms = C.int(options.RequestTimeoutMs)
	}
	if options.MaxReadRetries != 0 {
		cOptions.max_read_retries = C.int(options.MaxReadRetries)
	}
	if options.MaxWriteRetries != 0 {
		cOptions.max_write_retries = C.int(options.MaxWriteRetries)
	}
	if options.RetryBackoffMs != 0 {
		cOptions.retry_backoff_ms = C.int(options.RetryBackoffMs)
	}
	if options.MaxFeaturePointsPerRequest != 0 {
		cOptions.max_feature_points_per_request = C.int(options.MaxFeaturePointsPerRequest)
	}
	if options.MaxFeatureQueryCount != 0 {
		cOptions.max_feature_query_count = C.uint64_t(options.MaxFeatureQueryCount)
	}
	if options.MaxKeyBytes != 0 {
		cOptions.max_key_bytes = C.uint64_t(options.MaxKeyBytes)
	}
	if options.MaxValueBytes != 0 {
		cOptions.max_value_bytes = C.uint64_t(options.MaxValueBytes)
	}
	if options.AllowStaleReplicaReads {
		cOptions.pin_primary = 0
	} else if options.PinPrimary {
		cOptions.pin_primary = 1
	}

	var raw *C.temporalstore_client_t
	var errMsg *C.char
	if code := C.temporalstore_connect(&cOptions, &raw, &errMsg); code != 0 {
		return nil, takeError(errMsg)
	}
	return &Client{ptr: raw}, nil
}

func (c *Client) Close() error {
	if c == nil || c.ptr == nil {
		return nil
	}
	var errMsg *C.char
	code := C.temporalstore_close(c.ptr, &errMsg)
	c.ptr = nil
	if code != 0 {
		return takeError(errMsg)
	}
	return nil
}

func (c *Client) PutString(key, value string) error {
	cKey := C.CString(key)
	cValue := C.CString(value)
	defer C.free(unsafe.Pointer(cKey))
	defer C.free(unsafe.Pointer(cValue))
	var errMsg *C.char
	if code := C.temporalstore_put_string(c.ptr, cKey, cValue, &errMsg); code != 0 {
		return takeError(errMsg)
	}
	return nil
}

func (c *Client) PutStringWithTTL(key, value string, ttlMs uint64) error {
	cKey := C.CString(key)
	cValue := C.CString(value)
	defer C.free(unsafe.Pointer(cKey))
	defer C.free(unsafe.Pointer(cValue))
	var errMsg *C.char
	if code := C.temporalstore_put_string_with_ttl(
		c.ptr, cKey, cValue, C.uint64_t(ttlMs), &errMsg,
	); code != 0 {
		return takeError(errMsg)
	}
	return nil
}

func (c *Client) GetString(key string) (string, error) {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	var value *C.char
	var errMsg *C.char
	if code := C.temporalstore_get_string(c.ptr, cKey, &value, &errMsg); code != 0 {
		return "", takeError(errMsg)
	}
	defer C.temporalstore_free_string(value)
	return C.GoString(value), nil
}

func (c *Client) DeleteObject(key string) error {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	var errMsg *C.char
	if code := C.temporalstore_delete_object(c.ptr, cKey, &errMsg); code != 0 {
		return takeError(errMsg)
	}
	return nil
}

func (c *Client) Expire(key string, ttlMs uint64) error {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	var errMsg *C.char
	if code := C.temporalstore_expire(c.ptr, cKey, C.uint64_t(ttlMs), &errMsg); code != 0 {
		return takeError(errMsg)
	}
	return nil
}

func (c *Client) TTL(key string) (uint64, error) {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	var ttl C.uint64_t
	var errMsg *C.char
	if code := C.temporalstore_ttl(c.ptr, cKey, &ttl, &errMsg); code != 0 {
		return 0, takeError(errMsg)
	}
	return uint64(ttl), nil
}

func (c *Client) HSet(key, field, value string) error {
	cKey := C.CString(key)
	cField := C.CString(field)
	cValue := C.CString(value)
	defer C.free(unsafe.Pointer(cKey))
	defer C.free(unsafe.Pointer(cField))
	defer C.free(unsafe.Pointer(cValue))
	var errMsg *C.char
	if code := C.temporalstore_hset(c.ptr, cKey, cField, cValue, &errMsg); code != 0 {
		return takeError(errMsg)
	}
	return nil
}

func (c *Client) HGet(key, field string) (string, error) {
	cKey := C.CString(key)
	cField := C.CString(field)
	defer C.free(unsafe.Pointer(cKey))
	defer C.free(unsafe.Pointer(cField))
	var value *C.char
	var errMsg *C.char
	if code := C.temporalstore_hget(c.ptr, cKey, cField, &value, &errMsg); code != 0 {
		return "", takeError(errMsg)
	}
	defer C.temporalstore_free_string(value)
	return C.GoString(value), nil
}

func (c *Client) HDel(key, field string) error {
	cKey := C.CString(key)
	cField := C.CString(field)
	defer C.free(unsafe.Pointer(cKey))
	defer C.free(unsafe.Pointer(cField))
	var errMsg *C.char
	if code := C.temporalstore_hdel(c.ptr, cKey, cField, &errMsg); code != 0 {
		return takeError(errMsg)
	}
	return nil
}

func (c *Client) SAdd(key, member string) error {
	cKey := C.CString(key)
	cMember := C.CString(member)
	defer C.free(unsafe.Pointer(cKey))
	defer C.free(unsafe.Pointer(cMember))
	var errMsg *C.char
	if code := C.temporalstore_sadd(c.ptr, cKey, cMember, &errMsg); code != 0 {
		return takeError(errMsg)
	}
	return nil
}

func (c *Client) SMembers(key string) ([]string, error) {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	var out C.temporalstore_string_array_t
	var errMsg *C.char
	if code := C.temporalstore_smembers(c.ptr, cKey, &out, &errMsg); code != 0 {
		return nil, takeError(errMsg)
	}
	defer C.temporalstore_string_array_free(&out)
	if out.count == 0 {
		return nil, nil
	}
	rawValues := unsafe.Slice(out.values, out.count)
	values := make([]string, int(out.count))
	for i, value := range rawValues {
		values[i] = C.GoString(value)
	}
	return values, nil
}

func (c *Client) AddFeaturePoints(key string, points []FeaturePoint) error {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	cPoints, owned := makeCFeaturePoints(points)
	defer freeCStringList(owned)

	var pointPtr *C.temporalstore_feature_point_t
	if len(cPoints) > 0 {
		pointPtr = &cPoints[0]
	}
	var errMsg *C.char
	if code := C.temporalstore_add_feature_points(
		c.ptr, cKey, pointPtr, C.size_t(len(cPoints)), &errMsg,
	); code != 0 {
		return takeError(errMsg)
	}
	return nil
}

func (c *Client) QueryFeaturePoints(
	key string,
	startTs uint64,
	endTs uint64,
	count uint64,
	filters []FeatureFilter,
) ([]FeaturePoint, error) {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	cFilters, ownedFields := makeCFeatureFilters(filters)
	defer freeCStringList(ownedFields)
	var filterPtr *C.temporalstore_feature_filter_t
	if len(cFilters) > 0 {
		filterPtr = &cFilters[0]
	}

	var out C.temporalstore_feature_point_array_t
	var errMsg *C.char
	if code := C.temporalstore_query_feature_points_with_filters(
		c.ptr,
		cKey,
		C.uint64_t(startTs),
		C.uint64_t(endTs),
		C.uint64_t(count),
		filterPtr,
		C.size_t(len(cFilters)),
		&out,
		&errMsg,
	); code != 0 {
		return nil, takeError(errMsg)
	}
	defer C.temporalstore_feature_point_array_free(&out)
	if out.count == 0 {
		return nil, nil
	}
	rawPoints := unsafe.Slice(out.points, out.count)
	points := make([]FeaturePoint, int(out.count))
	for i, point := range rawPoints {
		points[i] = FeaturePoint{Timestamp: uint64(point.timestamp), Value: C.GoString(point.value)}
	}
	return points, nil
}

func (c *Client) AddSequenceFeatureRows(key string, rows []SequenceFeatureRow) error {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	cRows := make([]C.temporalstore_sequence_feature_row_t, len(rows))
	for i, row := range rows {
		cRows[i].timestamp = C.uint64_t(row.Timestamp)
		cRows[i].gid = C.uint64_t(row.GID)
		cRows[i].action_type = C.uint32_t(row.ActionType)
		cRows[i].duration = C.uint32_t(row.Duration)
		cRows[i].author_id = C.uint64_t(row.AuthorID)
	}
	var rowPtr *C.temporalstore_sequence_feature_row_t
	if len(cRows) > 0 {
		rowPtr = &cRows[0]
	}
	var errMsg *C.char
	if code := C.temporalstore_add_sequence_feature_rows(
		c.ptr, cKey, rowPtr, C.size_t(len(cRows)), &errMsg,
	); code != 0 {
		return takeError(errMsg)
	}
	return nil
}

func (c *Client) QuerySequenceFeatureRows(
	key string,
	startTs uint64,
	endTs uint64,
	count uint64,
	filters []FeatureFilter,
) ([]SequenceFeatureRow, error) {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	cFilters, ownedFields := makeCFeatureFilters(filters)
	defer freeCStringList(ownedFields)
	var filterPtr *C.temporalstore_feature_filter_t
	if len(cFilters) > 0 {
		filterPtr = &cFilters[0]
	}

	var out C.temporalstore_sequence_feature_row_array_t
	var errMsg *C.char
	if code := C.temporalstore_query_sequence_feature_rows(
		c.ptr,
		cKey,
		C.uint64_t(startTs),
		C.uint64_t(endTs),
		C.uint64_t(count),
		filterPtr,
		C.size_t(len(cFilters)),
		&out,
		&errMsg,
	); code != 0 {
		return nil, takeError(errMsg)
	}
	defer C.temporalstore_sequence_feature_row_array_free(&out)
	if out.count == 0 {
		return nil, nil
	}
	rawRows := unsafe.Slice(out.rows, out.count)
	rows := make([]SequenceFeatureRow, int(out.count))
	for i, row := range rawRows {
		rows[i] = SequenceFeatureRow{
			Timestamp:  uint64(row.timestamp),
			GID:        uint64(row.gid),
			ActionType: uint32(row.action_type),
			Duration:   uint32(row.duration),
			AuthorID:   uint64(row.author_id),
		}
	}
	return rows, nil
}

func (c *Client) AddIPSInstance(
	table string,
	uid int64,
	timestampUs int64,
	actionType int32,
	logicalTable int32,
	features []IpsFeatureStat,
) error {
	cTable := C.CString(table)
	defer C.free(unsafe.Pointer(cTable))
	cFeatures := make([]C.temporalstore_ips_feature_stat_t, len(features))
	for i, feature := range features {
		cFeatures[i].id = C.int64_t(feature.ID)
		cFeatures[i].slot = C.int32_t(feature.Slot)
		if feature.HasSlot {
			cFeatures[i].has_slot = 1
		}
		cFeatures[i]._type = C.int32_t(feature.Type)
		cFeatures[i].v1 = C.int32_t(feature.V1)
		cFeatures[i].v2 = C.int32_t(feature.V2)
	}
	var featurePtr *C.temporalstore_ips_feature_stat_t
	if len(cFeatures) > 0 {
		featurePtr = &cFeatures[0]
	}
	var errMsg *C.char
	if code := C.temporalstore_add_ips_instance(
		c.ptr,
		cTable,
		C.int64_t(uid),
		C.int64_t(timestampUs),
		C.int32_t(actionType),
		C.int32_t(logicalTable),
		featurePtr,
		C.size_t(len(cFeatures)),
		&errMsg,
	); code != 0 {
		return takeError(errMsg)
	}
	return nil
}

func (c *Client) QueryIPSLastInstances(
	table string,
	uid int64,
	actionType int32,
	logicalTable int32,
	slot int32,
	topK int32,
	lastInstances int64,
) ([]IpsFeatureStat, error) {
	cTable := C.CString(table)
	defer C.free(unsafe.Pointer(cTable))
	var out C.temporalstore_ips_feature_array_t
	var errMsg *C.char
	if code := C.temporalstore_query_ips_last_instances(
		c.ptr,
		cTable,
		C.int64_t(uid),
		C.int32_t(actionType),
		C.int32_t(logicalTable),
		C.int32_t(slot),
		C.int32_t(topK),
		C.int64_t(lastInstances),
		&out,
		&errMsg,
	); code != 0 {
		return nil, takeError(errMsg)
	}
	defer C.temporalstore_ips_feature_array_free(&out)
	if out.count == 0 {
		return nil, nil
	}
	rawFeatures := unsafe.Slice(out.features, out.count)
	features := make([]IpsFeatureStat, int(out.count))
	for i, feature := range rawFeatures {
		features[i] = IpsFeatureStat{
			ID:      int64(feature.id),
			Slot:    int32(feature.slot),
			HasSlot: feature.has_slot != 0,
			Type:    int32(feature._type),
			V1:      int32(feature.v1),
			V2:      int32(feature.v2),
		}
	}
	return features, nil
}

func (c *Client) RiskIncrement(
	key string,
	amount int64,
	ttlSeconds uint64,
	precision RiskPrecision,
	uuid string,
	occurTimeSeconds uint64,
) error {
	cKey := C.CString(key)
	cUUID := C.CString(uuid)
	defer C.free(unsafe.Pointer(cKey))
	defer C.free(unsafe.Pointer(cUUID))
	var errMsg *C.char
	if code := C.temporalstore_risk_increment(
		c.ptr,
		cKey,
		C.int64_t(amount),
		C.uint64_t(ttlSeconds),
		C.temporalstore_risk_precision_t(precision),
		cUUID,
		C.uint64_t(occurTimeSeconds),
		&errMsg,
	); code != 0 {
		return takeError(errMsg)
	}
	return nil
}

func (c *Client) RiskCount(
	key string,
	precision RiskPrecision,
	windowStart int64,
	windowEnd int64,
	windowUnit WindowUnit,
) (int64, error) {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	var count C.int64_t
	var errMsg *C.char
	if code := C.temporalstore_risk_count(
		c.ptr,
		cKey,
		C.temporalstore_risk_precision_t(precision),
		C.int64_t(windowStart),
		C.int64_t(windowEnd),
		C.temporalstore_window_unit_t(windowUnit),
		&count,
		&errMsg,
	); code != 0 {
		return 0, takeError(errMsg)
	}
	return int64(count), nil
}

func makeCFeaturePoints(points []FeaturePoint) ([]C.temporalstore_feature_point_t, []*C.char) {
	cPoints := make([]C.temporalstore_feature_point_t, len(points))
	owned := make([]*C.char, 0, len(points))
	for i, point := range points {
		cValue := C.CString(point.Value)
		owned = append(owned, cValue)
		cPoints[i].timestamp = C.uint64_t(point.Timestamp)
		cPoints[i].value = cValue
	}
	return cPoints, owned
}

func makeCFeatureFilters(filters []FeatureFilter) ([]C.temporalstore_feature_filter_t, []*C.char) {
	cFilters := make([]C.temporalstore_feature_filter_t, len(filters))
	owned := make([]*C.char, 0, len(filters))
	for i, filter := range filters {
		field := C.CString(filter.Field)
		owned = append(owned, field)
		cFilters[i].field = field
		cFilters[i].op = C.temporalstore_feature_filter_op_t(filter.Op)
		cFilters[i].value = C.uint64_t(filter.Value)
	}
	return cFilters, owned
}

func freeCStringList(values []*C.char) {
	for _, value := range values {
		C.free(unsafe.Pointer(value))
	}
}

func takeError(value *C.char) error {
	if value == nil {
		return errors.New("unknown TemporalStore error")
	}
	defer C.temporalstore_free_string(value)
	return errors.New(C.GoString(value))
}
