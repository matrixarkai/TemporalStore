import os
import random
import logging

from onebox import conf
from onebox import common

import sys
sys.path.append(os.environ["THRIFT_PATH"])

from onebox.bcache2_thrift.server.ttypes import *
import onebox.bcache2_thrift.server.Bcache2ThriftService as Bcache2ThriftService

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from servicediscovery import lookup_name


def setup():
    # wo do not need fault injection
    if conf.INJECT_FAULT:
        logging.info("pause fault injecter")
        conf.FAULT_INJECTER.pause()


def teardown():
    if conf.INJECT_FAULT:
        logging.info("resume fault injecter")
        conf.FAULT_INJECTER.resume()


def __new_thrift_client(consul, trans_protocol="Framed"):
    endpoint = random.choice(lookup_name(consul))
    logging.info("use proxy {}".format(endpoint))
    tsocket = TSocket.TSocket(conf.HOST_IP, endpoint["Port"])
    transport = None
    if trans_protocol == "Framed":
        transport = TTransport.TFramedTransport(tsocket)
    elif trans_protocol == "Buffered":
        transport = TTransport.TBufferedTransport(tsocket)
    else:
        assert False
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = Bcache2ThriftService.Client(protocol)
    transport.open()
    return client


def test_getset():
    namespace = conf.NAMESPACE
    ns_name = namespace['name']
    table_name = namespace['tables'][0]['name']
    consul = common.consul_name(namespace['proxy_groups'][0]['consul_name'])
    client = __new_thrift_client(consul)

    req = GetRequest(
        namespace_name=ns_name,
        table_name=table_name,
        key="not_exist".encode(),
    )
    resp = client.Get(req)
    logging.info("request {}, response {}".format(req, resp))
    assert resp.status.code == common.BCache2Code.NotFound

    req = SetRequest(
        namespace_name=ns_name,
        table_name=table_name,
        key="key".encode(),
        value="value".encode(),
    )
    resp = client.Set(req)
    logging.info("request {}, response {}".format(req, resp))
    assert resp.status.code == common.BCache2Code.OK

    req = GetRequest(
        namespace_name=ns_name,
        table_name=table_name,
        key="key".encode(),
    )
    resp = client.Get(req)
    logging.info("request {}, response {}".format(req, resp))
    assert resp.status.code == common.BCache2Code.OK
    assert resp.value == "value".encode()


def test_hmget():
    namespace = conf.NAMESPACE
    ns_name = namespace['name']
    table_name = namespace['tables'][0]['name']
    consul = common.consul_name(namespace['proxy_groups'][0]['consul_name'])
    client = __new_thrift_client(consul)

    # key not found
    req = HMGetRequest(
        namespace_name=ns_name,
        table_name=table_name,
        key="not_exist".encode(),
        fields=["fields1".encode(), "field2".encode()],
    )
    resp = client.HMGet(req)
    logging.info("request {}, response {}".format(req, resp))
    assert resp.status.code == common.BCache2Code.NotFound

    # set fields
    req = HMSetRequest(
        namespace_name=ns_name,
        table_name=table_name,
        key="test_hmget_key1".encode(),
        fields=["field111".encode(), "field222".encode()],
        values=["value111".encode(), "value222".encode()],
    )
    resp = client.HMSet(req)
    logging.info("request {}, response {}".format(req, resp))
    assert resp.status.code == common.BCache2Code.OK

    # all fields not exist
    req = HMGetRequest(
        namespace_name=ns_name,
        table_name=table_name,
        key="test_hmget_key1".encode(),
        fields=["field100".encode(), "field200".encode()],
    )
    resp = client.HMGet(req)
    logging.info("request {}, response {}".format(req, resp))
    assert resp.status.code == common.BCache2Code.OK
    assert len(resp.exists) == 2
    assert len(resp.values) == 2
    assert resp.exists[0] == False
    assert resp.exists[1] == False

    # some fields not exist
    req = HMGetRequest(
        namespace_name=ns_name,
        table_name=table_name,
        key="test_hmget_key1".encode(),
        fields=["field111".encode(), "field200".encode()],
    )
    resp = client.HMGet(req)
    logging.info("request {}, response {}".format(req, resp))
    assert resp.status.code == common.BCache2Code.OK
    assert len(resp.exists) == 2
    assert len(resp.values) == 2
    assert resp.exists[0] == True
    assert resp.exists[1] == False
    assert resp.values[0] == "value111".encode()

    # all fields exist
    req = HMGetRequest(
        namespace_name=ns_name,
        table_name=table_name,
        key="test_hmget_key1".encode(),
        fields=["field111".encode(), "field222".encode()],
    )
    resp = client.HMGet(req)
    logging.info("request {}, response {}".format(req, resp))
    assert resp.status.code == common.BCache2Code.OK
    assert len(resp.exists) == 2
    assert len(resp.values) == 2
    assert resp.exists[0] == True
    assert resp.exists[1] == True
    assert resp.values[0] == "value111".encode()
    assert resp.values[1] == "value222".encode()


def test_hmset():
    namespace = conf.NAMESPACE
    ns_name = namespace['name']
    table_name = namespace['tables'][0]['name']
    consul = common.consul_name(namespace['proxy_groups'][0]['consul_name'])
    client = __new_thrift_client(consul)

    # field and value not match
    req = HMSetRequest(
        namespace_name=ns_name,
        table_name=table_name,
        key="key".encode(),
        fields=["fields1".encode(), "field2".encode()],
        values=["value1".encode()],
    )
    resp = client.HMSet(req)
    logging.info("request {}, response {}".format(req, resp))
    assert resp.status.code == common.BCache2Code.InvalidArgument

    # hmset
    req = HMSetRequest(
        namespace_name=ns_name,
        table_name=table_name,
        key="test_hmget_key1".encode(),
        fields=["field111".encode(), "field222".encode()],
        values=["value111".encode(), "value222".encode()],
    )
    resp = client.HMSet(req)
    logging.info("request {}, response {}".format(req, resp))
    assert resp.status.code == common.BCache2Code.OK

    req = HMGetRequest(
        namespace_name=ns_name,
        table_name=table_name,
        key="test_hmget_key1".encode(),
        fields=["field111".encode(), "field222".encode()],
    )
    resp = client.HMGet(req)
    logging.info("request {}, response {}".format(req, resp))
    assert resp.status.code == common.BCache2Code.OK
    assert len(resp.exists) == 2
    assert len(resp.values) == 2
    assert resp.exists[0] == True
    assert resp.exists[1] == True
    assert resp.values[0] == "value111".encode()
    assert resp.values[1] == "value222".encode()


def test_hgetall():
    namespace = conf.NAMESPACE
    ns_name = namespace['name']
    table_name = namespace['tables'][0]['name']
    consul = common.consul_name(namespace['proxy_groups'][0]['consul_name'])
    client = __new_thrift_client(consul)

    # empty
    req = HGetAllRequest(
        namespace_name=ns_name,
        table_name=table_name,
        key="test_hgetall_key".encode(),
    )
    resp = client.HGetAll(req)
    logging.info("request {}, response {}".format(req, resp))
    assert resp.status.code == common.BCache2Code.NotFound
    req = HLenRequest(
        namespace_name=ns_name,
        table_name=table_name,
        key="test_hgetall_key".encode(),
    )
    resp = client.HLen(req)
    logging.info("request {}, response {}".format(req, resp))
    assert resp.status.code == common.BCache2Code.NotFound

    # hmset
    req = HMSetRequest(
        namespace_name=ns_name,
        table_name=table_name,
        key="test_hgetall_key".encode(),
        fields=["field111".encode(), "field222".encode()],
        values=["value111".encode(), "value222".encode()],
    )
    resp = client.HMSet(req)
    logging.info("request {}, response {}".format(req, resp))
    assert resp.status.code == common.BCache2Code.OK

    # hgetall
    req = HGetAllRequest(
        namespace_name=ns_name,
        table_name=table_name,
        key="test_hgetall_key".encode(),
    )
    resp = client.HGetAll(req)
    logging.info("request {}, response {}".format(req, resp))
    assert resp.status.code == common.BCache2Code.OK
    assert resp.fields[0] == "field111".encode()
    assert resp.fields[1] == "field222".encode()
    assert resp.values[0] == "value111".encode()
    assert resp.values[1] == "value222".encode()
    # hlen
    req = HLenRequest(
        namespace_name=ns_name,
        table_name=table_name,
        key="test_hgetall_key".encode(),
    )
    resp = client.HLen(req)
    logging.info("request {}, response {}".format(req, resp))
    assert resp.status.code == common.BCache2Code.OK
    assert resp.len == 2
