import logging

from onebox.components import http_client
from onebox import common
from bytedance import servicediscovery

class LeaderTracker(object):
    def __init__(self, cluster, uri):
        self.uri = uri
        self.cluster = cluster
        self.http_client = http_client.Client()
        self.leader = ""

    def get_leader(self):
        if self.leader:
            # self.query_leader(self.leader) throw exception
            y = False
            try:
                y = self.query_leader(self.leader)
            except:
                logging.info("leader is not connectable")
            if y:
                return self.leader
        common.check_with_retry(self.track_leader)
        return self.leader

    def parse_uri(self):
        consul_prefix = 'consul://'
        pieces = self.uri.split(',')
        endpoints = []
        for p in pieces:
            if consul_prefix in p:
                eps = servicediscovery.lookup_name(p[len(consul_prefix):])
                logging.info("find by consul, {} {}".format(p, eps))
                for ep in eps:
                    if ep['Host'].find(":") != -1:
                        # ipv6
                        endpoints.append("[{}]:{}".format(ep['Host'], ep['Port']))
                    else:
                        # ipv4
                        endpoints.append("{}:{}".format(ep['Host'], ep['Port']))
            else:
                endpoints.append(p)

        self.endpoints = endpoints
        logging.info("parse uri result, {}".format(self.endpoints))

    def query_leader(self, endpoint):
        params = {
            "id": {
                "cluster_name": self.cluster,
                "operator_name": "onebox"
            }
        }
        resp = self.http_client.post("http://{}/QueryService/QueryLeader".format(endpoint), params).json()
        logging.info("query leader, {} {}".format(endpoint, resp))
        return resp.get('is_leader', False)

    def track_leader(self):
        self.leader = ""
        self.parse_uri()
        for ep in self.endpoints:
            # self.query_leader(self.leader) throw exception
            y = False
            logging.info("try to check leader {}".format(ep))
            try:
                y = self.query_leader(ep)
            except:
                continue
            if y:
                self.leader = ep
                return True
        return False

class Client(object):
    def __init__(self, cluster, uri):
        self.cluster = cluster
        self.http_client = http_client.Client()
        self.leader_tracker = LeaderTracker(cluster, uri)

    def rpc(self, path, json={}, endpoint=""):
        if len(endpoint) == 0:
            endpoint = self.leader_tracker.get_leader()
        return self.http_client.post("http://{}/{}".format(endpoint, path), json).json()

    def add_namespace(self, name):
        return self.rpc("ManageService/AddNamespace", {
            "id": self.get_id(),
            "name": name,
            })

    def add_table(self, info):
        if 'id' not in info:
            info['id'] = self.get_id()
        return self.rpc("ManageService/AddTable", info)

    def drop_table(self, table_id):
        req = {
            'id': self.get_id(),
            'table_id': table_id
        }
        return self.rpc("ManageService/DropTable", req)

    def freeze_table(self, ns, name, table_id):
        req = {
            'id': self.get_id(),
            'namespace_name': ns,
            'name': name,
            'table_id': table_id
        }
        return self.rpc("ManageService/FreezeTable", req)

    def list_table(self, ns, table=None, read_stale=False, endpoint=""):
        req = {
            'id': self.get_id(),
            'read_stale': read_stale,
            'namespace_name': ns
        }
        if table:
            req['table_name'] = table
        return self.rpc("QueryService/ListTable", req, endpoint)

    def get_all_metaservers(self):
        self.leader_tracker.parse_uri()
        return self.leader_tracker.endpoints

    def put_proxy_group(self, req):
        if 'id' not in req:
            req['id'] = self.get_id()
        return self.rpc("ManageService/PutProxyGroup", req)

    def mute_meta_change(self):
        req = { 'id': self.get_id() }
        return self.rpc("ManageService/MuteMetaChange", req)

    def resume_meta_change(self):
        req = { 'id': self.get_id() }
        return self.rpc("ManageService/ResumeMetaChange", req)

    def list_proxy_group(self, ns, read_stale=False, endpoint=""):
        req = {
            'id': self.get_id(),
            'read_stale': read_stale,
            'namespace_name': ns
        }
        return self.rpc("QueryService/ListProxyGroup", req, endpoint)

    def freeze_server(self, id, reason):
        req = {
                'id': self.get_id(),
                'server_id': id,
                'reason': reason,
        }
        return self.rpc("ManageService/FreezeServer", req)

    def list_server(self, read_stale=False, endpoint=""):
        req = { 'id': self.get_id(),
                'read_stale': read_stale,
                "list_all_tag": True,
        }
        return self.rpc("QueryService/ListServer", req, endpoint)

    def list_proxy(self, read_stale=False, endpoint=""):
        req = { 'id': self.get_id(),
                'read_stale': read_stale
        }
        return self.rpc("QueryService/ListProxy", req, endpoint)

    def freeze_partition(self, pid):
        req = {
            'id': self.get_id(),
            'partition_id': pid,
        }
        return self.rpc("ManageService/FreezePartition", req)

    def list_partition(self, ns, table, read_stale=False, endpoint=""):
        req = { 'id': self.get_id(),
                'read_stale': read_stale,
                'namespace_name': ns,
                'table_name': table
        }
        return self.rpc("QueryService/ListPartition", req, endpoint)

    def get_id(self):
        return { "cluster_name": self.cluster,
                "operator_name": "onebox" }

    def get_template_for_add_table(self):
        partition_unit = {
            "partition_num": 2,
            "placement_set": [ {
                "vregion": "x",
                "vdc": "x",
                "vau": "x"
                } ],
            "storage_pool_uri": "x",
            "primary_prefer": {
                "vregion": "x",
                "vdc": "x",
                "vau": "x"
            }
        }
        quota = {
            "ops_read": 10000
        }
        config = {}
        return {
            "id": self.get_id(),
            "namespace_name": "x",
            "name": "y",
            "partition_set_num": 4,
            "partition_units": [ partition_unit ],
            "replication_union_policy": "ANTI_ENTROPY",
            "quota": quota,
            "config": config
        }

    def trigger_snapshot(self, endpoint=""):
        req = { 'id': self.get_id() }
        return self.rpc("RaftControlService/TriggerSnapshot", req, endpoint)
