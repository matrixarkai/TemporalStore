import re
import random

from onebox import conf

import requests
from bytedance import servicediscovery


def format_throughput(value):
    if value < 10 * 1024:
        # < 10K
        return format(value, ".2f") + "B/s"
    elif value < 10 * 1024 * 1024:
        # < 10M
        return format(value / 1024, ".2f") + "KB/s"
    elif value < 10 * 1024 * 1024 * 1024:
        # < 10G
        return format(value / 1024 / 1024, ".2f") + "MB/s"
    else:
        return format(value / 1024 / 1024 / 1024, ".2f") + "GB/s"


def gen_cluster_config():
    for consul in conf.METASERVER_URI.split(","):
        consul = consul[len("consul://"):]
        metaserver_ins = random.choice(servicediscovery.lookup_name(consul))
        if metaserver_ins["Host"].find(":") != -1 and metaserver_ins["Host"].find("[") == -1:
            metaserver_ins["Host"] = "[" + metaserver_ins["Host"] + "]"
        break
    resp = requests.post(
        url="http://{}:{}/QueryService/ListTable".format(metaserver_ins["Host"], metaserver_ins["Port"]),
        json={
            "id": {
                "operator_name": "chaos_report",
                "cluster_name": conf.METASERVER_CLUSTER_NAME,
            },
            "namespace_name": conf.NAMESPACE["name"],
            "table_name": conf.NAMESPACE["tables"][0]["name"],
            "read_stale": True,
        },
    ).json()

    config = resp["tables"][0]["config"]
    maxmemory = config["evicter_config"]["maxmemory"]["value"]

    resp = requests.post(
        url="http://{}:{}/QueryService/ListServer".format(metaserver_ins["Host"], metaserver_ins["Port"]),
        json={
            "id": {
                "operator_name": "chaos_report",
                "cluster_name": conf.METASERVER_CLUSTER_NAME,
            },
            "read_stale": True,
        },
    ).json()
    server = random.choice(resp["servers"])
    resp = requests.get(
        url="http://[{}]:{}/flags".format(server["server_info"]["endpoint"]["ip6"], server["server_info"]["endpoint"]["port"]),
        headers={
            "User-Agent": "curl/7.52.1",
        },
    )
    storage_async = re.search(r"storage_async.+?\| (.+?) ", resp.text).group(1)
    storage_oplog_delay_dump_length = int(re.search(r"storage_oplog_delay_dump_length.+?\| (.+?) ", resp.text).group(1))
    storage_gc_space_utility_threshold = float(re.search(
        r"storage_gc_space_utility_threshold.+?\| (.+?) ", resp.text).group(1))

    # and we need markdown format string
    ret = "maxmemory: {}MB".format(maxmemory//1024//1024)
    ret += "\nstorage_async: {}".format(storage_async)
    ret += "\nstorage_oplog_delay_dump_length: {}MB".format(format_throughput(storage_oplog_delay_dump_length))
    ret += "\nstorage_gc_space_utility_threshold: {}".format(format(storage_gc_space_utility_threshold, '.2f'))
    return ret


def gen_cluster_metrics():
    session = requests.session()
    resp = session.post(
        url="{}/api/access_token?_region=${}".format(conf.METRICS_QUERY_HOST, conf.METRICS_REGION),
        json={
            "app_name": conf.METRICS_APP_NAME,
            "app_secret": conf.METRICS_APP_SECRET,
        },
    ).json()
    session.headers["Authorization"] = resp["access_token"]

    # cpu metrics
    resp = session.post(
        url="{}/api/expr".format(conf.METRICS_BOSUN_HOST),
        params={
            "_region": conf.METRICS_REGION,
        },
        data='avg(q("avg:bcache2.server.cpu{{dc=*}}{{cluster={}}}", "10m", "2m"))'.format(conf.METASERVER_CLUSTER_NAME)
    ).json()
    idcs = list(map(lambda x: x["Group"]["dc"], resp["Results"]))
    values = list(map(lambda x: format(x["Value"], '.02f'), resp["Results"]))
    message = "Server cpu使用率(平均值): {}: {}%".format("/".join(idcs), "/".join(values))

    # memory metrics
    resp = session.post(
        url="{}/api/expr".format(conf.METRICS_BOSUN_HOST),
        params={
            "_region": conf.METRICS_REGION,
        },
        data='avg(q("avg:bcache2.server.memory_usage.mb{{dc=*}}{{cluster={}}}", "10m", "2m"))'.format(conf.METASERVER_CLUSTER_NAME)
    ).json()
    idcs = list(map(lambda x: x["Group"]["dc"], resp["Results"]))
    values = list(map(lambda x: format(x["Value"], '.02f')+"MB", resp["Results"]))
    message += "\nServer memory使用率(平均值): {}: {}".format("/".join(idcs), "/".join(values))

    # Index rewrite qps
    resp = session.post(
        url="{}/api/expr".format(conf.METRICS_BOSUN_HOST),
        params={"_region": conf.METRICS_REGION, },
        data='avg(q("sum:bcache2.server.partition.index.gc.rewrite_qps{{dc=lf}}{{cluster={}}}", "10m", "2m"))'.format(
            conf.METASERVER_CLUSTER_NAME)).json()
    if "Results" in resp and resp["Results"] is None:
        value = "Null"
    else:
        value = format(resp["Results"][0]["Value"], ".0f")
    message += "\nIndexGC rewrite item qps(总): {}".format(value)

    # Page gc rewrite throughput
    resp = session.post(
        url="{}/api/expr".format(conf.METRICS_BOSUN_HOST),
        params={"_region": conf.METRICS_REGION, },
        data='avg(q("sum:bcache2.server.partition.page_gc.rewrite_throughput{{dc=lf}}{{cluster={}}}", "10m", "2m"))'.
        format(conf.METASERVER_CLUSTER_NAME)).json()
    if "Results" in resp and resp["Results"] is None:
        value = "Null"
    else:
        value = format_throughput(resp["Results"][0]["Value"])
    message += "\nPageGC rewrite throughput(总): {}".format(value)

    # Page compaction load/dump page throughput
    resp = session.post(
        url="{}/api/expr".format(conf.METRICS_BOSUN_HOST),
        params={"_region": conf.METRICS_REGION, },
        data='avg(q("sum:bcache2.server.partition.page_compaction.load_page_throughput{{dc=lf}}{{cluster={}}}", "10m", "2m"))'.
        format(conf.METASERVER_CLUSTER_NAME)).json()
    if "Results" in resp and resp["Results"] is None:
        load_throughput = "Null"
    else:
        load_throughput = format_throughput(resp["Results"][0]["Value"])
    resp = session.post(
        url="{}/api/expr".format(conf.METRICS_BOSUN_HOST),
        params={"_region": conf.METRICS_REGION, },
        data='avg(q("sum:bcache2.server.partition.page_compaction.dump_page_throughput{{dc=lf}}{{cluster={}}}", "10m", "2m"))'.
        format(conf.METASERVER_CLUSTER_NAME)).json()
    if "Results" in resp and resp["Results"] is None:
        dump_throughput = "Null"
    else:
        dump_throughput = format_throughput(resp["Results"][0]["Value"])
    message += "\nPage compaction load/dump page throughput(总): {}/{}".format(load_throughput, dump_throughput)

    # Slot dump/load qps
    resp = session.post(
        url="{}/api/expr".format(conf.METRICS_BOSUN_HOST),
        params={"_region": conf.METRICS_REGION, },
        data='avg(q("sum:bcache2.server.partition.slot.load.qps{{dc=lf}}{{cluster={}}}", "10m", "2m"))'.format(
            conf.METASERVER_CLUSTER_NAME)).json()
    if "Results" in resp and resp["Results"] is None:
        load_qps = "Null"
    else:
        load_qps = format(resp["Results"][0]["Value"], ".0f")
    resp = session.post(
        url="{}/api/expr".format(conf.METRICS_BOSUN_HOST),
        params={"_region": conf.METRICS_REGION, },
        data='avg(q("sum:bcache2.server.partition.slot.dump.qps{{dc=lf}}{{cluster={}}}", "10m", "2m"))'.format(
            conf.METASERVER_CLUSTER_NAME)).json()
    if "Results" in resp and resp["Results"] is None:
        dump_qps = "Null"
    else:
        dump_qps = format(resp["Results"][0]["Value"], ".0f")
    message += "\nSlot load/dump qps(总): {}/{}".format(load_qps, dump_qps)

    # Evict/Expire object qps
    resp = session.post(
        url="{}/api/expr".format(conf.METRICS_BOSUN_HOST),
        params={"_region": conf.METRICS_REGION, },
        data='avg(q("sum:bcache2.server.partition.evict.object_qps{{dc=lf}}{{cluster={}}}", "10m", "2m"))'.format(
            conf.METASERVER_CLUSTER_NAME)).json()
    if "Results" in resp and resp["Results"] is None:
        evict_qps = "Null"
    else:
        evict_qps = format(resp["Results"][0]["Value"], ".0f")
    resp = session.post(
        url="{}/api/expr".format(conf.METRICS_BOSUN_HOST),
        params={"_region": conf.METRICS_REGION, },
        data='avg(q("sum:bcache2.server.partition.expirer.expire_key.count{{dc=lf}}{{cluster={}}}", "10m", "2m"))'.
        format(conf.METASERVER_CLUSTER_NAME)).json()
    if "Results" in resp and resp["Results"] is None:
        expire_qps = "Null"
    else:
        expire_qps = format(resp["Results"][0]["Value"], ".0f")
    message += "\nEvict/Expire object qps(总): {}/{}".format(evict_qps, expire_qps)

    return message


def gen_client_metrics():
    session = requests.session()
    resp = session.post(
        url="{}/api/access_token?_region=${}".format(conf.METRICS_QUERY_HOST, conf.METRICS_REGION),
        json={
            "app_name": conf.METRICS_APP_NAME,
            "app_secret": conf.METRICS_APP_SECRET,
        },
    ).json()
    session.headers["Authorization"] = resp["access_token"]

    # Client qps
    resp = session.post(
        url="{}/api/expr".format(conf.METRICS_BOSUN_HOST),
        params={"_region": conf.METRICS_REGION, },
        data='avg(q("sum:bcache2.client.table.cmd.success{{}}{{table_name={}}}", "10m", "2m"))'.format(
            conf.NAMESPACE["name"] + "/" + conf.NAMESPACE["tables"][0]["name"])).json()
    if "Results" in resp and resp["Results"] is None:
        value = "Null"
    else:
        value = format(resp["Results"][0]["Value"], ".0f")
    message = "Client qps: {}".format(value)

    # Client latency
    resp = session.post(
        url="{}/api/expr".format(conf.METRICS_BOSUN_HOST),
        params={"_region": conf.METRICS_REGION, },
        data='avg(q("avg:bcache2.client.table.cmd.latency.avg{{}}{{table_name={}}}", "10m", "2m"))'.format(
            conf.NAMESPACE["name"] + "/" + conf.NAMESPACE["tables"][0]["name"])).json()
    if "Results" in resp and resp["Results"] is None:
        avg = "Null"
    else:
        avg = format(resp["Results"][0]["Value"] / 1000, ".2f")
    resp = session.post(
        url="{}/api/expr".format(conf.METRICS_BOSUN_HOST),
        params={"_region": conf.METRICS_REGION, },
        data='avg(q("avg:bcache2.client.table.cmd.latency.pct50{{}}{{table_name={}}}", "10m", "2m"))'.format(
            conf.NAMESPACE["name"] + "/" + conf.NAMESPACE["tables"][0]["name"])).json()
    if "Results" in resp and resp["Results"] is None:
        p50 = "Null"
    else:
        p50 = format(resp["Results"][0]["Value"] / 1000, ".2f")
    resp = session.post(
        url="{}/api/expr".format(conf.METRICS_BOSUN_HOST),
        params={"_region": conf.METRICS_REGION, },
        data='avg(q("avg:bcache2.client.table.cmd.latency.pct99{{}}{{table_name={}}}", "10m", "2m"))'.format(
            conf.NAMESPACE["name"] + "/" + conf.NAMESPACE["tables"][0]["name"])).json()
    if "Results" in resp and resp["Results"] is None:
        p99 = "Null"
    else:
        p99 = format(resp["Results"][0]["Value"] / 1000, ".1f")
    message += "\nClient latency avg/p50/p99: {}/{}/{}ms".format(avg, p50, p99)

    return message


if __name__ == "__main__":
    print(gen_cluster_config())
    print(gen_cluster_metrics())
    print(gen_client_metrics())
