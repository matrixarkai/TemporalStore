import sys
import json
import socket
import logging

from ipaddress import ip_address, IPv4Address

from onebox import common
from onebox.components import etcd
from onebox.components import brpc_stub

import servicediscovery

def check_port_used(ip, port):
    address_family = socket.AF_INET if type(ip_address(ip)) is IPv4Address else socket.AF_INET6
    sock = socket.socket(address_family, socket.SOCK_STREAM)
    sock.settimeout(0.1)
    result = sock.connect_ex((ip, port))
    sock.close()
    return True if result == 0 else False


def check_etcd_ready(ip, port):
    client = etcd.Client(host=ip, port=port)
    try:
        client.put("sniffer", "sniffer")
    except Exception as ex:
        logging.debug("etcd set failed: {}".format(ex))
        return False
    else:
        return True



def check_server_ready(ip, port):
    return check_port_used(ip, port)


def check_proxy_ready(ip, port):
    return check_port_used(ip, port)


def check_server_dead(ip, port):
    return not check_port_used(ip, port)


def check_partition_ready(server_ip, server_port, partition_id):
    client = brpc_stub.Client(server_ip, server_port)
    resp = client.call("ServerService", "GetInfo", {
        "partition_id": partition_id,
    })
    if resp["status"].get("code", common.BCache2Code.OK) != common.BCache2Code.OK:
        logging.debug("invalid response code {}".format(resp["status"].get("code", common.BCache2Code.OK)))
        return False
    if resp["partition_info"]["state"] != common.BCache2PartitionState.LOADED.name:
        logging.debug("invalid partition state {}".format(resp["partition_info"]["state"]))
        return False
    return True


def check_partition_not_exist(server_ip, server_port, partition_id):
    client = brpc_stub.Client(server_ip, server_port)
    resp = client.call("ServerService", "GetInfo", {
        "partition_id": partition_id,
    })
    if resp["status"].get("code", common.BCache2Code.OK) != common.BCache2Code.NotFound:
        logging.debug("invalid response code {}".format(resp["status"].get("code", common.BCache2Code.OK)))
        return False
    return True


def check_consul_ready(consul):
    endpoints = servicediscovery.lookup_name(consul)
    if len(endpoints) == 0:
        logging.debug("consul is empty. consul {}".format(consul))
        return False
    return True

def check_table_created(ms_client, ns, table):
    resp = ms_client.list_table(ns, table)
    # logging.info('list table resp: {}'.format(resp))
    if 'tables' not in resp:
        return False
    for t in resp['tables']:
        if t['name'] != table:
            continue
        return t['state'] == 'TABLE_NORMAL'
    return False

def check_meta_server_crash_consistency(servera, serverb, proxya, proxyb, tablea, tableb, partitiona, partitionb):
    if len(servera["servers"]) != len(serverb["servers"]):
        logging.info('server number not equal servera: {} serverb: {} '.format(servera, serverb))
        return False
    if len(proxya["proxies"]) != len(proxyb["proxies"]):
        logging.info('proxy number not equal proxya: {} proxyb: {} '.format(proxya, proxyb))
        return False
    if len(tablea["tables"]) != len(tableb["tables"]):
        logging.info('table number not equal tablea: {} tableb: {} '.format(tablea, tableb))
        return False
    if len(partitiona["info"]) != len(partitionb["info"]):
        logging.info('partition number not equal partitiona: {} partitionb: {} '
                     .format(partitiona, partitionb))
        return False

    check_success = True

    servera["servers"] = sorted(servera["servers"], key=lambda x:x["server_info"]["id"])
    serverb["servers"] = sorted(serverb["servers"], key=lambda x:x["server_info"]["id"])
    for i in range(len(servera["servers"])):
        if servera["servers"][i]["server_info"] != serverb["servers"][i]["server_info"]:
            check_success = False
            break
        if servera["servers"][i]["node_info"] != serverb["servers"][i]["node_info"]:
            check_success = False
            break

    if not check_success:
        logging.info('server info not match servera: {} serverb: {} '.format(len(servera), len(serverb)))
        return False

    proxya["proxies"] = sorted(proxya["proxies"], key=lambda x:x["proxy_info"]["id"])
    proxyb["proxies"] = sorted(proxyb["proxies"], key=lambda x:x["proxy_info"]["id"])
    for i in range(len(proxya["proxies"])):
        if proxya["proxies"][i]["proxy_info"] != proxyb["proxies"][i]["proxy_info"]:
            check_success = False
            break
        if proxya["proxies"][i]["namespace_name"] != proxyb["proxies"][i]["namespace_name"]:
            check_success = False
            break
        if proxya["proxies"][i]["config"] != proxyb["proxies"][i]["config"]:
            check_success = False
            break

    if not check_success:
        logging.info('proxy info not match proxya: {} proxyb: {} '.format(len(proxya), len(proxyb)))
        return False

    tablea["tables"] = sorted(tablea["tables"], key=lambda x:x["id"])
    tableb["tables"] = sorted(tableb["tables"], key=lambda x:x["id"])
    if tablea["tables"] != tableb["tables"]:
        logging.info('table info not equal tablea: {} tableb: {} '.format(tablea, tableb))
        return False

    partitiona["info"] = sorted(partitiona["info"], key=lambda x:x["set_info"]["id"])
    partitionb["info"] = sorted(partitionb["info"], key=lambda x:x["set_info"]["id"])

    for i in range(len(partitiona["info"])):
        pset_a = partitiona['info'][i]
        pset_b = partitionb['info'][i]
        if pset_a != pset_b:
            if pset_a['partition_info'] != pset_b['partition_info']:
                pset_a["partition_info"] = sorted(pset_a["partition_info"], key=lambda x:x["id"])
                pset_b["partition_info"] = sorted(pset_b["partition_info"], key=lambda x:x["id"])
                if pset_a['partition_info'] != pset_b['partition_info']:
                    logging.info('pset_a info not equal pset_a: {} pset_b: {} '
                                 .format(pset_a['partition_info'], pset_b['partition_info']))
                    return False
    return True
