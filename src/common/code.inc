// FATAL: DO NOT change the code number

// for rpc
DEFINE_CODE(OK, 0)
DEFINE_CODE(Cancelled, 1)
DEFINE_CODE(Unknown, 2)
DEFINE_CODE(InvalidArgument, 3)
DEFINE_CODE(DeadlineExceeded, 4)
DEFINE_CODE(NotFound, 5)
DEFINE_CODE(AlreadyExists, 6)
DEFINE_CODE(PermissionDenied, 7)
DEFINE_CODE(ResourceExhausted, 8)
DEFINE_CODE(FailedPrecondition, 9)
DEFINE_CODE(Aborted, 10)
DEFINE_CODE(OutOfRange, 11)
DEFINE_CODE(Unimplemented, 12)
DEFINE_CODE(Internal, 13)
DEFINE_CODE(Unavailable, 14)
DEFINE_CODE(DataLoss, 15)
DEFINE_CODE(Unauthenticated, 16)
DEFINE_CODE(Unmatched, 17)

// for partition
DEFINE_CODE(TopomError, 18)
DEFINE_CODE(PartitionLoading, 19)
DEFINE_CODE(ZoneChanged, 20)
DEFINE_CODE(MetaChanged, 21)
DEFINE_CODE(RetryLater, 22)
DEFINE_CODE(Continue, 23)
DEFINE_CODE(ResourceFrozen, 24)
DEFINE_CODE(FaultInjection, 25)

// for store
DEFINE_CODE(StoreConditionFailed, 26)
DEFINE_CODE(StoreNotFound, 27)
DEFINE_CODE(StoreInternal, 28)

// for stream
DEFINE_CODE(StreamAbort, 29)

// for model
DEFINE_CODE(NoAction, 30)

// for risk model
DEFINE_CODE(RiskAlreadyHandled, 31)
