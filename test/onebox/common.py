import enum
import time
import random
import logging
import string

from onebox import conf
from onebox import check_helper


class BCache2Code(enum.IntEnum):
    OK = 0,
    Cancelled = 1,
    Unknown = 2,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    NotFound = 5,
    AlreadyExists = 6,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    FailedPrecondition = 9,
    Aborted = 10,
    OutOfRange = 11,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    DataLoss = 15,
    Unauthenticated = 16,
    Unmatched = 17,
    TopomError = 18,
    PartitionLoading = 19,
    MetaChanged = 20,


class BCache2PartitionState(enum.IntEnum):
    INIT = 0,
    LOADING = 1,
    LOADED = 2,
    UNLOADING = 3,
    UNLOADED = 4,


def check_with_retry(func, *args, max_try=60, interval=1, on_error_func=None):
    for _ in range(max_try):
        if func(*args):
            return True

        time.sleep(interval)

    logging.error("check {}() failed".format(func.__name__))

    if on_error_func is not None:
        on_error_func()

    return False


def get_unused_port():
    while True:
        port = random.randint(2024, 9000)
        if check_helper.check_port_used(conf.HOST_IP, port):
            continue
        return port


def split_addr(addr):
    if addr.find("[") == -1:
        # ipv4
        return addr.split(":")
    else:
        # ipv6
        idx = addr.rfind(addr)
        return addr[:idx], addr[addr+1:]

CONSUL_SUFFIX = ''.join(random.choice(string.ascii_lowercase) for i in range(7))
def consul_name(prefix):
    return "{}_{}".format(prefix, CONSUL_SUFFIX)
