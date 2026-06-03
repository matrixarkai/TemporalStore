import atexit
import logging
import threading

from signal import pause
from onebox import conf

logging.basicConfig(
    level=logging._nameToLevel[conf.LOG_LEVEL],
    format="%(asctime)s (%(thread)d) [%(filename)s:%(lineno)d] %(levelname)s - %(message)s",
)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


def setup_package():
    logging.info("deploying, deployer type is {}".format(conf.DEPLOYER_TYPE))
    conf.DEPLOYER.setup()
    if conf.SETUP_ONLY:
        logging.info("setup finish, pausing")
        atexit.register(teardown_package)
        pause()

    logging.info("start fault injecter")
    if conf.INJECT_FAULT:
        conf.FAULT_INJECTER.start()


def teardown_package():
    if conf.INJECT_FAULT:
        logging.info("stop fault injecter")
        conf.FAULT_INJECTER.stop()
        logging.info("waiting fault injecter stop")
        conf.FAULT_INJECTER.join()

    if not conf.WITHOUT_TEARDOWN:
        conf.DEPLOYER.teardown()