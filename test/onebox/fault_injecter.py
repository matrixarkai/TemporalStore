import json
import re
import time
import random
import logging
import threading
import requests
import json
import copy

from onebox import deployer
from onebox.components import wukong
from onebox.components import metaserver_client
from onebox import common
from onebox import check_helper
from onebox import conf

# {prefix}/{injection_point}/{type}, e.g., store/bytestore/io/write/hang
FIU_GROUPS = {
    "bytestore": {
        "prefix": "store/bytestore",
        "injection_points": [
            "io/write",
            "io/async_write",
            "io/read",
            "io/async_read",
            "ioctl/open",
            "ioctl/close",
            "ioctl/stat",
            "ioctl/delete",
            "ioctl/rename",
            "ioctl/create_inline_blob",
            "ioctl/update_inline_blob",
            "ioctl/stat_inline_blob",
            "ioctl/open_pool",
            "ioctl/traverse_pool",
            "ioctl/close_pool",
        ],
        "types": ["hang", "failure", "crash", ],
    },
    "bytestore_data_distort": {
        "prefix": "store/bytestore",
        "injection_points": [
            "io/async_read",
        ],
        "types": ["data_distort", ],
    },
    "oplog_gc": {
        "prefix": "oplog_gc/reclaim_oplog",
        "injection_points": [
            "after_slot_store_dump",
            "after_index_set_dumped_log_id",
        ],
        "types": ["hang", "crash", ],
    },
    "index_gc": {
        "prefix": "index_gc",
        "injection_points": [
            "try_gc/after_meta_update",
            "try_gc/after_rewrite_index_log",
            "try_gc/after_rewrite_object_log",
            "reclaim_index/after_try_gc",
            "reclaim_index/after_commit_dirty_slots",
            "reclaim_index/after_index_truncate",
        ],
        "types": ["hang", "crash", ],
    },
    "page_gc": {
        "prefix": "page_gc",
        "injection_points": [
            "purge_compacted_zones/after_destroy_zone",
            "purge_compacted_zones/after_oplog_truncate",
            "gc_current_zone/after_slot_store_commit_dirty_slots",
            "gc_current_zone/after_slot_store_load_dump_pages",
            "gc_current_zone/in_recycle_zone",
        ],
        "types": ["hang", "crash", ],
    },
    "dump": {
        "prefix": "slot_store/dump_slot_pages",
        "injection_points": [
            "after_index_clear_slot_dirty",
            "after_page_store_commit",
            "after_oplogger_commit",
            "after_index_update_slot_pages_and_metas",
        ],
        "types": ["hang", "crash", ],
    },
}


class FaultInjecter(threading.Thread):
    def __init__(
            self, deployer, metaserver: metaserver_client.Client, wukong: wukong.ByteChaosClient, interval_s,
            max_duration_s, fault_types, code_fiu_config, product):
        super(FaultInjecter, self).__init__()
        self.stop_event = threading.Event()
        self.pause_event = threading.Event()
        self.interval_s = interval_s
        self.daemon = True
        self.deployer = deployer
        self.wukong = wukong
        self.metaserver_client = metaserver
        self.max_duration_s = max_duration_s
        self.fault_types = fault_types
        self.code_fiu_config = code_fiu_config
        self.product_name = product
        self.launch_time = time.time()
        self.inject_num = 0

        fault_maps = {
            'SERVER_DOWN': self.__mock_server_down,
            'FREEZE_PARTITION': self.__mock_freeze_partition,
            'FREEZE_SERVER': self.__mock_freeze_server,
            'META_SERVER_DOWN': self.__mock_meta_server_down,
            'SERVER_HANG': self.__mock_server_hang,
            'NETWORK_DELAY': self.__mock_network_delay,
            'NETWORK_DROP': self.__mock_network_drop,
            'NETWORK_REJECT': self.__mock_network_reject,
            'CODE_FIU': self.__code_fiu,
            'DISK_BUSY': self.__mock_disk_busy,
            'TIME_SKEW': self.__mock_time_skew,
        }
        self.fault_actions = [fault_maps[x] for x in self.fault_types]

    def pause(self):
        self.pause_event.set()

    def resume(self):
        self.pause_event.clear()

    def stop(self):
        self.stop_event.set()

    def join(self):
        super(FaultInjecter, self).join()

    def get_inject_num(self):
        return self.inject_num

    def clear_inject_num(self):
        self.inject_num = 0

    def get_fault_types(self):
        return self.fault_types

    def run(self):
        while not self.stop_event.is_set():
            if self.pause_event.is_set():
                logging.debug("waiting for resume")
                time.sleep(1)
                continue
            if time.time() - self.launch_time < 60:
                logging.debug("do not inject fault in first 60s")
                time.sleep(1)
                continue

            picked_fault = random.choice(self.fault_actions)
            logging.info("pick fault {}".format(picked_fault))
            picked_fault()
            self.inject_num += 1

            for _ in range(self.interval_s):
                if self.stop_event.is_set():
                    break
                time.sleep(1)

    def __pick_server(self, server_type=""):
        if server_type == "":
            server_type = random.choice(["meta_server", "partition_server"])
        if server_type == "meta_server":
            metaservers = list(map(lambda ins: {"ip": ins.rpartition(":")[0].strip(
                "[]"), "port": ins.rpartition(":")[2]}, self.metaserver_client.get_all_metaservers()))
            metaserver = random.choice(metaservers)
            service_name = "bcache2_metaserver_{}.service".format(self.metaserver_client.cluster)
            logging.info("pick metaserver {}".format(json.dumps(metaserver)))
            return metaserver, service_name, metaservers
        else:
            resp = self.metaserver_client.list_server()
            endpoints = list(map(lambda ins: ins["server_info"]["endpoint"],
                                 filter(lambda ins: ins["server_info"]["state"] == "SERVER_NORMAL", resp["servers"])))
            servers = []
            for endpoint in endpoints:
                servers.append({"ip": endpoint["ip6"].strip("[]") if endpoint["addr_family"]
                                                                     == "ADDR_V6" else endpoint["ip4"], "port": endpoint["port"]})
            server = random.choice(servers)
            service_name = "bcache2_server_{}_{}.service".format(self.metaserver_client.cluster, server["port"])
            logging.info("pick server {}".format(json.dumps(server)))
            return server, service_name, servers

    def __mock_server_down(self):
        server, service_name, _ = self.__pick_server("partition_server")
        logging.info("mock server {} down".format(server))

        ip = server["ip"]
        port = server["port"]
        if not self.deployer.restart_server(ip, int(port), service_name):
            logging.warn("failed to stop server")
            return

    # TODO(yunxiao): optimize this func
    def add_table(self, allow_add=True):
        table_info = copy.deepcopy(conf.NAMESPACE['tables'][0])
        for unit in table_info['partition_units']:
            if unit['storage_pool_uri'] == "":
                unit['storage_pool_uri'] = "file://{}/data/".format(conf.ONEBOX_DIR)
        table_info["name"] = "table" + str(random.randint(100,1000000))

        resp = self.metaserver_client.add_table(table_info)
        if  allow_add:
            assert 'code' not in resp['status'] or resp['status']['code'] == 0
            time.sleep(7)
            common.check_with_retry(check_helper.check_table_created,
                                conf.METASERVER, table_info['namespace_name'], table_info['name'])
        else:
            assert 'code' not in resp['status'] or resp['status']['message'] == 'meta change muted'

    # TODO(yunxiao): optimize this func
    def drop_table(self):
        tables = self.metaserver_client.list_table(conf.NAMESPACE['name'])
        for table in tables["tables"]:
            id = int(table["id"])
            if id == 1:
                continue
            if (table["state"] == "TABLE_NORMAL"):
                frez_res = self.metaserver_client.freeze_table(table["namespace_name"], table["name"], id)
                logging.info("frez table res: {}".format(frez_res))
                assert 'code' not in frez_res['status'] or frez_res['status']['code'] == 0
            elif (table["state"] == "TABLE_FROZEN"):
                drop_res = self.metaserver_client.drop_table(id)
                logging.info("drop table res: {}".format(drop_res))
                assert 'code' not in drop_res['status'] or drop_res['status']['code'] == 0

    def __mock_freeze_partition(self):
        tables = self.metaserver_client.list_table(conf.NAMESPACE['name'])
        for table in tables["tables"]:
            if table["state"] != "TABLE_NORMAL":
                continue
            partitions = self.metaserver_client.list_partition(conf.NAMESPACE['name'], conf.NAMESPACE['tables'][0]['name'])
            psets = partitions["info"]
            random.shuffle(psets)
            freeze_count = 1
            for i in range(freeze_count):
                ps = psets[i]['partition_info']
                for p in ps:
                    if p['state'] == "P_NORMAL":
                        logging.info("random freeze partition, pid: {}".format(p['id']))
                        self.metaserver_client.freeze_partition(p['id'])
                        break


    def __mock_meta_server_down(self):
        # random pick a meta server to restart & check its crash consistency
        meta_server, service_name, _ = self.__pick_server("meta_server")
        logging.info("mock meta_server {} down".format(meta_server))
        ip = meta_server["ip"]
        port = meta_server["port"]
        if ip.find(":") != -1:
            endpoint = "[" + ip + "]:" + port
        else:
            endpoint = ip + ":" + port

        # do and mute meta change then take a snapshot
        self.add_table(True)
        self.metaserver_client.mute_meta_change()
        self.add_table(False)

        # trigger snapshot
        self.metaserver_client.trigger_snapshot(endpoint)
        time.sleep(5)

        # save the meta data before meta server restart
        servers1 = self.metaserver_client.list_server(True, endpoint)
        proxies1 = self.metaserver_client.list_proxy(True, endpoint)
        partitions1 = self.metaserver_client.list_partition(
            conf.NAMESPACE['name'], conf.NAMESPACE['tables'][0]['name'], True, endpoint)
        tables1 = self.metaserver_client.list_table(conf.NAMESPACE['name'], None, True, endpoint)

        # systemctl would restart server after 5s
        self.deployer.restart_metaserver(ip, int(port), service_name, 3)

        # fetch and check the meta data after meta server restart
        servers2 = self.metaserver_client.list_server(True, endpoint)
        proxies2 = self.metaserver_client.list_proxy(True, endpoint)
        partitions2 = self.metaserver_client.list_partition(
            conf.NAMESPACE['name'], conf.NAMESPACE['tables'][0]['name'], True, endpoint)
        tables2 = self.metaserver_client.list_table(conf.NAMESPACE['name'], None, True, endpoint)

        if not check_helper.check_meta_server_crash_consistency(
                servers1, servers2, proxies1, proxies2, tables1, tables2, partitions1, partitions2):
            logging.warn("meta server crash recovery consistency check failed")
            assert False

        # fetch meta data from leader and compare it with current meta server
        servers3 = self.metaserver_client.list_server()
        proxies3 = self.metaserver_client.list_proxy()
        partitions3 = self.metaserver_client.list_partition(conf.NAMESPACE['name'], conf.NAMESPACE['tables'][0]['name'])
        tables3 = self.metaserver_client.list_table(conf.NAMESPACE['name'])

        if not check_helper.check_meta_server_crash_consistency(
                servers2, servers3, proxies2, proxies3, tables2, tables3, partitions2, partitions3):
            logging.warn("meta server crash recovery consistency check failed")
            assert False

        self.metaserver_client.resume_meta_change()
        self.drop_table()

    def __mock_freeze_server(self):
        server, service_name, _ = self.__pick_server("partition_server")
        duration_s = random.randint(1, self.max_duration_s)
        resp = self.metaserver_client.list_server()
        ids = list(map(lambda ins: ins["server_info"]["id"],
                                 filter(lambda ins: ins["server_info"]["state"] == "SERVER_NORMAL", resp["servers"])))

        random.shuffle(ids)
        logging.info("mock freeze server {}".format(ids[0]))
        self.metaserver_client.freeze_server(ids[0], "MAINTAIN")

    def __mock_server_hang(self):
        server, service_name, _ = self.__pick_server("partition_server")
        duration_s = random.randint(1, self.max_duration_s)
        logging.info("mock server {} hang, duration_s {}".format(server, duration_s))

        ip = server["ip"]
        port = server["port"]
        if not self.deployer.hang_server(ip, int(port), service_name, duration_s):
            logging.warn("failed to hang server")
            return

    def __mock_network_reject(self):
        probability = random.randint(1, 5)
        duration_s = random.randint(1, self.max_duration_s)
        server, _, servers = self.__pick_server()
        logging.info("mock server {} network reject, probability {} duration_s {}".format(
            server, probability, duration_s))

        ip = server["ip"]
        servers_ip = list(map(lambda server: server["ip"], servers))
        if not self.deployer.network_reject(ip, servers_ip, probability, duration_s):
            logging.warn("failed to mock network reject")

    def __mock_network_drop(self):
        probability = random.randint(1, 5)
        duration_s = random.randint(1, self.max_duration_s)
        server, _, servers = self.__pick_server()
        logging.info("mock server {} network drop, probability {}, duration_s {}".format(
            server, probability, duration_s))

        ip = server["ip"]
        servers_ip = list(map(lambda server: server["ip"], servers))
        if not self.deployer.network_drop(ip, servers_ip, probability, duration_s):
            logging.warn("failed to mock network drop")

    def __mock_network_delay(self):
        delay_ms = random.randint(100, 5000)
        duration_s = random.randint(1, self.max_duration_s)
        server, _, servers = self.__pick_server()
        logging.info("mock server {} network delay, delay_ms {} duration_s {}".format(server, delay_ms, duration_s))

        ip = server["ip"]
        servers_ip = list(map(lambda server: server["ip"], servers))
        if not self.deployer.network_delay(ip, servers_ip, delay_ms, duration_s):
            logging.warn("failed to mock network delay")

    def __code_fiu(self):
        # merge default config and the chosen group config
        fault_group_config = {**self.code_fiu_config["DEFAULT"], **random.choice(self.code_fiu_config["GROUPS"])}
        assert "GROUP" in fault_group_config.keys() and fault_group_config["GROUP"] in FIU_GROUPS.keys()
        fault_group = FIU_GROUPS[fault_group_config["GROUP"]]
        # pick a bunch of servers
        _, _, all_servers = self.__pick_server("partition_server")
        random.shuffle(all_servers)
        picked_servers_cnt = min(
            random.randint(fault_group_config["MIN_SERVERS"],
                           fault_group_config["MAX_SERVERS"]),
            len(all_servers)
        )
        picked_servers = all_servers[:picked_servers_cnt]
        # pick injection point that is matched by regex
        injection_point = random.choice(list(
            filter(lambda ins: re.match(fault_group_config["INJECTION_POINTS"], ins),
                   fault_group["injection_points"])))
        # pick injection type that is matched by regex
        type = random.choice(list(
            filter(lambda ins: re.match(fault_group_config["TYPES"], ins), fault_group["types"])))

        full_injection_point = "{}/{}/{}".format(fault_group["prefix"], injection_point, type)
        for server in picked_servers:
            ip, port = (server["ip"], server["port"])
            probability = random.randint(
                fault_group_config["MIN_PROBABILITY"],
                fault_group_config["MAX_PROBABILITY"])
            duration_s = random.randint(
                fault_group_config["MIN_DURATION_S"],
                fault_group_config["MAX_DURATION_S"])
            success = self.deployer.inject_code_fault(ip, port, full_injection_point, probability, duration_s)
            if not success:
                logging.warn("failed to inject fault: ip={}, port={}, fault={}, prob={}, duration={}".format(
                    ip, port, full_injection_point, probability, duration_s))
        logging.info("Inject {} faults in group {} to {} servers".format(
            full_injection_point, fault_group_config["GROUP"], picked_servers_cnt))

    def __mock_disk_busy(self):
        duration_s = random.randint(1, self.max_duration_s)
        server, _, _ = self.__pick_server("meta_server")  # meta server uses local disk
        file_path = "/data00/bcache2_metaserver_meta_chaos/raft_wal/"
        io_type = random.choice(["read", "write"])
        logging.info("mock metaserver {} disk busy, io_type:{}, path:{}, duration_s:{}".format(
            server, io_type, file_path, duration_s))

        ip = server["ip"]
        if not deployer.RemoteDeployer.disk_busy(ip, file_path, io_type, duration_s):
            logging.warn("failed to mock disk busy")

    def __mock_time_skew(self):
        duration_s = random.randint(1, self.max_duration_s)
        offset = random.choice(["+", "-"]) + str(random.randint(1, 100)) + random.choice(["s", "m", "h"])
        server, _, _ = self.__pick_server()
        logging.info("mock metaserver {} time skew, offset:{}, duration_s:{}".format(
            server, offset, duration_s))

        ip = server["ip"]
        if not deployer.RemoteDeployer.time_skew(ip, offset, duration_s):
            logging.warn("failed to mock disk busy")

    def __check_target_wukong_agent(self, target_ip):
        if target_ip.find(":") != -1:
            # ipv6
            target_ip = "[" + target_ip + "]"
        try:
            url = "http://{}:1977/status".format(target_ip)
            payload = {}
            headers = {}
            response = requests.request("GET", url, headers=headers, data=payload)
            obj = json.loads(response.text)
        except:
            return False
        return "code" in obj and obj["code"] == 0

    def check_wukong_agent(self):
        _, _, meta_servers = self.__pick_server("meta_server")
        _, _, partition_servers = self.__pick_server("partition_server")
        servers = meta_servers + partition_servers
        for server in servers:
            if not self.__check_target_wukong_agent(server["ip"]):
                logging.error("no wukong agent on server:{}".format(server))
                return False
        return True
