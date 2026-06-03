import json
import logging

from onebox import conf
from onebox import common
from onebox import check_helper


def setup():
    # wo do not need fault injection
    if conf.INJECT_FAULT:
        logging.info("pause fault injecter")
        conf.FAULT_INJECTER.pause()


def teardown():
    if conf.INJECT_FAULT:
        logging.info("resume fault injecter")
        conf.FAULT_INJECTER.resume()

# TODO(wuzhenyu): add more cases
