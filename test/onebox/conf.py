import os
import time
import random

from onebox import common
from onebox import deployer
from onebox import fault_injecter
from onebox.components import lark
from onebox.components import wukong
from onebox.components import metaserver_client

from testconfig import config


# global
HOST_IP4 = os.getenv("BYTED_HOST_IP", "127.0.0.1")
HOST_IP6 = os.getenv("BYTED_HOST_IPV6", "::1")
if os.getenv("BYTED_HOST_IP") != "":
    HOST_IP = "127.0.0.1"
elif os.getenv("BYTED_HOST_IPV6") != "":
    HOST_IP = "[0:0:0:0:0:0:0:1]"
CURRENT_DIR = os.getcwd()
ONEBOX_DIR = os.path.join(CURRENT_DIR, "onebox_env")
TOOLS_DIR = os.path.join(CURRENT_DIR, "output", "third")
IDC = config["IDC"]
SETUP_ONLY = config["SETUP_ONLY"]
INJECT_FAULT = config["INJECT_FAULT"]
CASE_ROUNDS = config["CASE_ROUNDS"]
WITHOUT_TEARDOWN = config["WITHOUT_TEARDOWN"]
LOG_LEVEL = config["LOG_LEVEL"]
DEPLOYER_TYPE = config["DEPLOYER_TYPE"]
AUTH_TOKEN = config["AUTH_TOKEN"]
PROC_TIMEOUT = config["PROC_TIMEOUT"]

# lark api
LARK_APP_ID = config["LARK_APP_ID"]
LARK_APP_TOKEN = config["LARK_APP_TOKEN"]
LARK_CHAT_ID = config["LARK_CHAT_ID"]
LARK_TEMPLATE_ID = config["LARK_TEMPLATE_ID"]
LARK = lark.Lark(LARK_APP_ID, LARK_APP_TOKEN)

# metrics api
METRICS_QUERY_HOST = config["METRICS_QUERY_HOST"]
METRICS_BOSUN_HOST = config["METRICS_BOSUN_HOST"]
METRICS_REGION = config["METRICS_REGION"]
METRICS_APP_NAME = config["METRICS_APP_NAME"]
METRICS_APP_SECRET = config["METRICS_APP_SECRET"]

# metaserver
METASERVER_LOCAL_BIN = config['METASERVER']['LOCAL_BIN']
METASERVER_PORT = common.get_unused_port()
METASERVER_FLAGS = config['METASERVER']['FLAGS']
METASERVER_CLUSTER_NAME = METASERVER_FLAGS['metaserver_cluster_name']
METASERVER_NUM = config["METASERVER"]["NUM"]

if DEPLOYER_TYPE == "local":
    METASERVER_FLAGS['metaserver_announce_consul_name'] = common.consul_name(METASERVER_FLAGS['metaserver_announce_consul_name'])
    METASERVER_FLAGS['metaserver_announce_consul_name_leader'] = common.consul_name(METASERVER_FLAGS['metaserver_announce_consul_name_leader'])
    METASERVER_ADDR = '[::1]:{}'.format(METASERVER_PORT) # for setup_benchmark
    METASERVER_URI = '[::1]:{},consul://{}'.format(METASERVER_PORT, METASERVER_FLAGS['metaserver_announce_consul_name'])
else:
    METASERVER_URI = ""
    for consul in METASERVER_FLAGS['metaserver_announce_consul_name'].split(","):
        if len(METASERVER_URI) == 0:
            METASERVER_URI = "consul://{}".format(consul)
            METASERVER_ADDR = consul
        else:
            METASERVER_URI += ",consul://{}".format(consul)
METASERVER = metaserver_client.Client(METASERVER_CLUSTER_NAME, METASERVER_URI)

# server
SERVER_LOCAL_BIN = config["SERVER"]["LOCAL_BIN"]
SERVER_SCM_REPO = config["SERVER"]["SCM_REPO"]
SERVER_SCM_VERSION = config["SERVER"]["SCM_VERSION"]
SERVER_NUM = config["SERVER"]["NUM"]
SERVER_BEGIN_PORT = config["SERVER"]["BEGIN_PORT"]
SERVER_BYTEBOX_ID = config["SERVER"]["BYTEBOX_ID"]
SERVER_BYTEBOX_NAME = config["SERVER"]["BYTEBOX_NAME"]
SERVER_MASTER_CONSUL = config["SERVER"]["MASTER_CONSUL"]
SERVER_FLAGS = config["SERVER"]["FLAGS"]
SERVER_LOCATIONS = config['SERVER']['LOCATIONS']

# proxy
if DEPLOYER_TYPE == "local":
    PROXY_LOCAL_BIN = config["PROXY"]["LOCAL_BIN"]
    PROXY_FLAGS = config["PROXY"]["FLAGS"]
    PROXY_NUM = config["PROXY"]["NUM"]
    PROXY_LOCATIONS = config['PROXY']['LOCATIONS']

# benchmark
BENCHMARK_FROM_SCM = config["BENCHMARK"].get("FROM_SCM", False)
BENCHMARK_SCM_REPO = config["BENCHMARK"].get("SCM_REPO", "")
BENCHMARK_SCM_VERSION = config["BENCHMARK"].get("SCM_VERSION", 0)
BENCHMARK_PRODUCT_NAME = config["BENCHMARK"].get("REMOTE_PRODUCT_NAME", "")

BENCHMARK_LOCAL_BIN = config["BENCHMARK"]["LOCAL_BIN"]
BENCHMARK_ROUND = config["BENCHMARK"]["ROUND"]
BENCHMARK_PORT = config["BENCHMARK"].get("PORT", 0)
BENCHMARK_FLAGS = config["BENCHMARK"]["FLAGS"]

# sla
SLA_TOTAL_QPS = config["SLA"]["TOTAL"]["QPS"]
SLA_TOTAL_AVG_LATENCY_MS = config["SLA"]["TOTAL"]["AVG_LATENCY_MS"]
SLA_TOTAL_P99_LATENCY_MS = config["SLA"]["TOTAL"]["P99_LATENCY_MS"]
SLA_TOTAL_AVAILABILITY = config["SLA"]["TOTAL"]["AVAILABILITY"]
SLA_COMMAND = config["SLA"]["COMMAND"]

# wukong
WUKONG = wukong.ByteChaosClient()

# deployer
if DEPLOYER_TYPE == "local":
    DEPLOYER = deployer.LocalDeployer()
else:
    DEPLOYER = deployer.RemoteDeployer()

NAMESPACE = config['NAMESPACE']

# fault injection
FAULT_INJECTER = fault_injecter.FaultInjecter(
    DEPLOYER, METASERVER, WUKONG, config["FAULT_INJECTER"]["FAULT_INTERVAL"],
    config["FAULT_INJECTER"]["FAULT_MAX_DURATION"], config["FAULT_INJECTER"]['FAULT_TYPES'],
    config["CODE_FIU_CONFIG"], BENCHMARK_PRODUCT_NAME)
