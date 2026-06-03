package temporalstore

type FeatureFilterOp int

const (
	FeatureFilterEqual       FeatureFilterOp = 0
	FeatureFilterNotEqual    FeatureFilterOp = 1
	FeatureFilterGreaterThan FeatureFilterOp = 2
	FeatureFilterLessThan    FeatureFilterOp = 3
)

type RiskPrecision int

const (
	RiskOneSecond   RiskPrecision = 0
	RiskFiveSeconds RiskPrecision = 1
	RiskTenSeconds  RiskPrecision = 2
	RiskOneMinute   RiskPrecision = 3
	RiskFiveMinutes RiskPrecision = 4
	RiskTenMinutes  RiskPrecision = 5
	RiskOneHour     RiskPrecision = 6
	RiskOneDay      RiskPrecision = 7
	RiskOneMonth    RiskPrecision = 8
)

type WindowUnit int

const (
	WindowSecond WindowUnit = 0
	WindowMinute WindowUnit = 1
	WindowHour   WindowUnit = 2
	WindowDay    WindowUnit = 3
)

type Options struct {
	MetaserverAddr             string
	MetaserverConsul           string
	NamespaceName              string
	TableName                  string
	IDC                        string
	Host                       string
	PSM                        string
	LogDir                     string
	LogLevel                   int
	IOTimeoutMs                int
	ConnectTimeoutMs           int
	RequestTimeoutMs           int
	MaxReadRetries             int
	MaxWriteRetries            int
	RetryBackoffMs             int
	MaxFeaturePointsPerRequest int
	MaxFeatureQueryCount       uint64
	MaxKeyBytes                uint64
	MaxValueBytes              uint64
	PinPrimary                 bool
	AllowStaleReplicaReads     bool
}

type FeatureFilter struct {
	Field string          `json:"field"`
	Op    FeatureFilterOp `json:"op"`
	Value uint64          `json:"value"`
}

type FeaturePoint struct {
	Timestamp uint64 `json:"timestamp"`
	Value     string `json:"value"`
}

type SequenceFeatureRow struct {
	Timestamp  uint64 `json:"timestamp"`
	GID        uint64 `json:"gid"`
	ActionType uint32 `json:"action_type"`
	Duration   uint32 `json:"duration"`
	AuthorID   uint64 `json:"author_id"`
}

type IpsFeatureStat struct {
	ID      int64 `json:"id"`
	Slot    int32 `json:"slot"`
	HasSlot bool  `json:"has_slot"`
	Type    int32 `json:"type"`
	V1      int32 `json:"v1"`
	V2      int32 `json:"v2"`
}
