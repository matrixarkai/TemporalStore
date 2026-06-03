import os
import time
import yaml
import shutil
import threading
import logging
import json

from onebox import conf
from onebox import common
from onebox import commands
from onebox import check_helper
from onebox.components import etcd
from onebox.components import tao
from onebox.components import wukong
from onebox.components import metaserver_client

class LocalDeployer():
    @staticmethod
    def setup():
        logging.info("kill the old onebox processes")
        for process_dir in os.listdir(conf.ONEBOX_DIR) if os.path.exists(conf.ONEBOX_DIR) else []:
            commands.kill_process_by_name(process_dir)

        logging.info("recreate {}".format(conf.ONEBOX_DIR))
        shutil.rmtree(conf.ONEBOX_DIR, ignore_errors=True)
        os.makedirs(conf.ONEBOX_DIR)
        LocalDeployer.setup_cluster()

    @staticmethod
    def teardown():
        logging.info("kill the onebox processes")
        for process_dir in os.listdir(conf.ONEBOX_DIR) if os.path.exists(conf.ONEBOX_DIR) else []:
            commands.kill_process_by_name(process_dir)

    @staticmethod
    def setup_cluster():
        namespace = conf.NAMESPACE
        logging.info("setup metaserver, count: {}".format(conf.METASERVER_NUM))

        # Generate raft peers list.
        peers = ""
        for i in range(conf.METASERVER_NUM - 1):
            peers += "{},{}:{},{}:{},0,".format(i, conf.HOST_IP, common.get_unused_port(),\
                                                conf.HOST_IP, common.get_unused_port())
        if conf.METASERVER_NUM >= 1:
            peers += "{},{}:{},{}:{},0".format(conf.METASERVER_NUM - 1, conf.HOST_IP,\
                                               common.get_unused_port(), conf.HOST_IP, common.get_unused_port())
        logging.info(peers)

        for i in range(conf.METASERVER_NUM - 1):
            port = common.get_unused_port()
            conf.METASERVER_URI = conf.METASERVER_URI + ",[::1]:{}".format(port)
            LocalDeployer.setup_metaserver(i, port, peers)

        if conf.METASERVER_NUM >= 1:
            LocalDeployer.setup_metaserver(conf.METASERVER_NUM - 1, conf.METASERVER_PORT, peers)

        conf.METASERVER = metaserver_client.Client(conf.METASERVER_CLUSTER_NAME, conf.METASERVER_URI)
        conf.FAULT_INJECTER.metaserver_client = conf.METASERVER

        logging.info("setup servers, count: {}".format(conf.SERVER_NUM))
        for i in range(conf.SERVER_NUM):
            port = common.get_unused_port()
            LocalDeployer.setup_server(i, port)

        logging.info("setup proxies, count: {}".format(conf.PROXY_NUM))
        for i in range(conf.PROXY_NUM):
            port = common.get_unused_port()
            LocalDeployer.setup_proxy(i, port)

        logging.info("check servers registered")
        server_added = False
        for _ in range(20):
            time.sleep(1)
            resp = conf.METASERVER.list_server()
            if 'servers' in resp and len(resp['servers']) == conf.SERVER_NUM:
                server_added = True
                break
        assert server_added, "servers are still not registered in metaserver"
        logging.info("check proxy registered")
        server_added = False
        for _ in range(20):
            time.sleep(1)
            resp = conf.METASERVER.list_proxy()
            if 'proxies' in resp and len(resp['proxies']) == conf.PROXY_NUM:
                server_added = True
                break
        assert server_added, "proxies are still not registered in metaserver"
        logging.info("add namespace")
        resp = conf.METASERVER.add_namespace(namespace['name'])
        assert 'code' not in resp['status'] or resp['status']['code'] == 0, "failed to add namespace, got {}".format(resp)

        logging.info("add proxy group")
        for proxy_group in namespace['proxy_groups']:
            req = {
                'info': {
                    'namespace_name': namespace['name'],
                    'instance_num': proxy_group['instance_num'],
                    'placement': proxy_group['placement'],
                    'config': {
                        'consul_names': [ common.consul_name(proxy_group['consul_name']) ]
                        }
                    }
            }
            resp = conf.METASERVER.put_proxy_group(req)
            assert 'code' not in resp['status'] or resp['status']['code'] == 0, "failed to put proxy group, got {}".format(resp)

        logging.info("add table")
        for table in namespace['tables']:
            for unit in table['partition_units']:
                if unit['storage_pool_uri'] == "":
                    unit['storage_pool_uri'] = "file://{}/data/".format(conf.ONEBOX_DIR)
            resp = conf.METASERVER.add_table(table)
            assert 'code' not in resp['status'] or resp['status']['code'] == 0, "failed to add table, got {}".format(resp)

        logging.info("check table is created")
        time.sleep(5)
        for table in namespace['tables']:
            common.check_with_retry(check_helper.check_table_created, conf.METASERVER, table['namespace_name'], table['name'])
        logging.info("check consul is announced")
        for proxy_group in namespace['proxy_groups']:
            consul_name = common.consul_name(proxy_group['consul_name'])
            common.check_with_retry(check_helper.check_consul_ready, consul_name)

    @staticmethod
    def setup_metaserver(idx, port, peers):
        logging.info("create metaserver, port:{}".format(port))
        bin_file = "metaserver_{}".format(port)
        server_dir = "{}/{}".format(conf.ONEBOX_DIR, bin_file)
        os.makedirs(server_dir)
        src = "{}/{}".format(conf.CURRENT_DIR, conf.METASERVER_LOCAL_BIN)
        dst = "{}/{}".format(server_dir, bin_file)
        logging.info("link local metaserver bin file, src {}, dst {}".format(src, dst))
        os.link(src, dst)

        logging.info("start metaserver")
        flags = ' '.join(['--{}={}'.format(flag, value) for flag, value in conf.METASERVER_FLAGS.items()])
        logging.info("start metaserver, flags {}".format(flags))
        with open(server_dir + "/start.sh", "w") as fd:
            fd.write("""ulimit -c unlimited
                        export ASAN_OPTIONS=abort_on_error=1:disable_coredump=0:unmap_shadow_on_exit=1
                        export ASAN_OPTIONS=$ASAN_OPTIONS:verify_asan_link_order=0
                        timeout {timeout} ./{bin} \\
                        --metaserver_server_port={port} \\
                        --metaserver_work_dir={work_dir} \\
                        --metaserver_log_dir={log_dir} \\
                        --metaserver_proxy_calibrate_interval_ms=100 \\
                        --metaserver_raft_heartbeat_cycle_ms=1500 \\
                        --metaserver_raft_election_cycle_ms=3000 \\
                        --metaserver_raft_id={idx} \\
                        --metaserver_raft_peers={peers} \\
                        --metaserver_frozen_partition_cool_down_time_sec=300 \\
                        --metaserver_raft_segment_size=16384 \\
                        --crash_on_fatal_log=true \\
                        --metaserver_raft_max_applied_log_bytes=32768 \\
                        --metaserver_snapshot_trigger_interval_sec=300 \\
                        --metaserver_snapshot_trigger_index_gap=2 \\
                        {flags} >stdout 2>stderr &
                    """.format(
                bin=bin_file,
                port=port,
                work_dir=os.path.join(server_dir, "data"),
                log_dir=os.path.join(server_dir, "log"),
                flags=flags,
                timeout=conf.PROC_TIMEOUT,
                idx=idx,
                peers=peers,
            ))
            os.chmod(server_dir + "/start.sh", 0o755)
        commands.run_shell("cd {} && ./start.sh".format(server_dir))
        assert common.check_with_retry(check_helper.check_port_used, conf.HOST_IP, port, interval=0.1)

    @staticmethod
    def start_metaserver(ip, port):
        logging.info("start meta server {}".format(port))
        bin_file = "metaserver_{}".format(port)
        server_dir = "{}/{}".format(conf.ONEBOX_DIR, bin_file)
        commands.run_shell("cd {} && ./start.sh".format(server_dir))
        assert common.check_with_retry(check_helper.check_server_ready, ip, port)

    @staticmethod
    def stop_metaserver(ip, port, service_name=""):
        logging.info("[Instance stop] {}, meta_server_{}".format(conf.METASERVER_CLUSTER_NAME, port))
        commands.kill_process_by_name("metaserver_{}".format(port))
        assert common.check_with_retry(check_helper.check_server_dead, ip, port)

    @staticmethod
    def restart_metaserver(ip, port, service_name="", stop_s=0):
        LocalDeployer.stop_metaserver(ip, port)
        time.sleep(stop_s)
        LocalDeployer.start_metaserver(ip, port)

    @staticmethod
    def setup_server(idx, port):
        logging.info("create server dir for server {}".format(port))
        bin_file = "server_{}".format(port)
        server_dir = "{}/{}".format(conf.ONEBOX_DIR, bin_file)
        os.makedirs(server_dir)
        src = "{}/{}".format(conf.CURRENT_DIR, conf.SERVER_LOCAL_BIN)
        dst = "{}/{}".format(server_dir, bin_file)
        logging.info("link local server bin file, src {}, dst {}".format(src, dst))
        os.link(src, dst)
        server_libs = conf.CURRENT_DIR+"/output/lib"
        os.makedirs(server_dir+"/lib")
        for file in os.listdir(server_libs):
            if file.endswith(".so"):
                src = "{}/{}".format(conf.CURRENT_DIR, "output/lib/" + file)
                dst = "{}/{}".format(server_dir, "lib/" + file)
                logging.info("link local lib file, src {}, dst {}".format(src, dst))
                os.link(src, dst)

        logging.info("prepare host spec file")
        host_spec_path = os.path.join(server_dir, "host_spec.json")
        spec = {}
        endpoint = {
                "ip4": conf.HOST_IP4,
                "ip6": conf.HOST_IP6,
                "port": port,
                }
        if endpoint['ip4'] and endpoint['ip6']:
            endpoint['addr_family'] = 2
        elif endpoint['ip6']:
            endpoint['addr_family'] = 1
        spec['endpoint'] = endpoint
        spec['location'] = conf.SERVER_LOCATIONS[idx % len(conf.SERVER_LOCATIONS)]
        with open(host_spec_path, 'w') as f:
            f.write(json.dumps(spec, indent=4))

        logging.info("start server")
        flags = ' '.join(['--{}={}'.format(flag, value) for flag, value in conf.SERVER_FLAGS.items()])
        logging.info("start server, flags {}".format(flags))
        with open(server_dir + "/start.sh", "w") as fd:
            fd.write("""ulimit -c unlimited
                    export ASAN_OPTIONS=abort_on_error=1:disable_coredump=0:unmap_shadow_on_exit=1
                    export ASAN_OPTIONS=$ASAN_OPTIONS:verify_asan_link_order=0
                    timeout {timeout} ./{bin} \\
                    --cluster_name={cluster} \\
                    --metaserver_uri={metaserver_uri} \\
                    --host_spec_path={host_spec_path} \\
                    --port={port} \\
                    --storage_async=false \\
                    {flags} >stdout 2>stderr &
                """.format(bin=bin_file,
                           port=port,
                           cluster=conf.METASERVER_CLUSTER_NAME,
                           metaserver_uri=conf.METASERVER_URI,
                           host_spec_path=host_spec_path,
                           log_level=conf.LOG_LEVEL,
                           flags=flags,
                           timeout=conf.PROC_TIMEOUT,
                           ))
            os.chmod(server_dir + "/start.sh", 0o755)
        commands.run_shell("cd {} && ./start.sh".format(server_dir))
        assert common.check_with_retry(check_helper.check_server_ready, conf.HOST_IP, port, interval=0.1)

    @staticmethod
    def setup_proxy(idx, port):
        logging.info("create proxy dir for proxy {}".format(port))
        bin_file = "proxy_{}".format(port)
        proxy_dir = "{}/{}".format(conf.ONEBOX_DIR, bin_file)
        os.makedirs(proxy_dir)

        src = "{}/{}".format(conf.CURRENT_DIR, conf.PROXY_LOCAL_BIN)
        dst = "{}/{}".format(proxy_dir, bin_file)
        logging.info("link local proxy bin file, src {}, dst {}".format(src, dst))
        os.link(src, dst)

        libs = conf.CURRENT_DIR+"/output/lib"
        os.makedirs(proxy_dir+"/lib")
        for file in os.listdir(libs):
            if file.endswith(".so"):
                src = "{}/{}".format(conf.CURRENT_DIR, "output/lib/" + file)
                dst = "{}/{}".format(proxy_dir, "lib/" + file)
                logging.info("link local lib file, src {}, dst {}".format(src, dst))
                os.link(src, dst)

        logging.info("start proxy")
        flags = ' '.join(['--{}={}'.format(flag, value) for flag, value in conf.PROXY_FLAGS.items()])
        logging.info("start proxy, flags {}".format(flags))
        loc = conf.PROXY_LOCATIONS[idx % len(conf.PROXY_LOCATIONS)]
        with open(proxy_dir + "/start.sh", "w") as fd:
            fd.write("""ulimit -c unlimited
                    export ASAN_OPTIONS=abort_on_error=1:disable_coredump=0:unmap_shadow_on_exit=1
                    timeout {timeout} ./{bin} \\
                    --port={port} \\
                    --idc={idc} \\
                    --proxy_cluster_name={cluster} \\
                    --proxy_vregion={vregion} \\
                    --proxy_vdc={vdc} \\
                    --proxy_vau={vau} \\
                    --master_endpoint={master} \\
                    --metaserver_uri={metaserver_uri} \\
                    {flags} >stdout 2>stderr &
                """.format(bin=bin_file,
                           port=port,
                           cluster=conf.METASERVER_CLUSTER_NAME,
                           vregion=loc['vregion'],
                           vdc=loc['vdc'],
                           vau=loc['vau'],
                           idc=loc['vdc'],
                           master=conf.METASERVER_ADDR,
                           metaserver_uri=conf.METASERVER_URI,
                           flags=flags,
                           timeout=conf.PROC_TIMEOUT,
                           ))
            os.chmod(proxy_dir + "/start.sh", 0o755)
        commands.run_shell("cd {} && ./start.sh".format(proxy_dir))
        assert common.check_with_retry(check_helper.check_proxy_ready, conf.HOST_IP, port, interval=0.1)

    @staticmethod
    def setup_benchmark(namespace, table_name, benchmark_name, port, flags):
        ms_addr = conf.METASERVER_ADDR
        logging.info("create benchmark dir for port {}, ms {}".format(port, ms_addr))
        bin_file = benchmark_name
        server_dir = "{}/{}".format(conf.ONEBOX_DIR, bin_file)
        if conf.DEPLOYER_TYPE == "remote" and os.path.exists(server_dir):
            commands.run_shell("cd {} && ./start.sh".format(server_dir))
            assert common.check_with_retry(check_helper.check_port_used, conf.HOST_IP, port, interval=2)
            return

        shutil.rmtree(server_dir, ignore_errors=True)
        os.makedirs(server_dir)
        commands.kill_process_by_name(bin_file)
        if conf.BENCHMARK_FROM_SCM:
            logging.info("pull benchmark bin file")
            commands.bvc_pull(conf.BENCHMARK_SCM_REPO, conf.BENCHMARK_SCM_VERSION,
                              "{}/bcache2-benchmark".format(server_dir))
            found = False
            for item in os.listdir("{}/bcache2-benchmark/".format(server_dir)):
                if item.startswith("bcache2-bench"):
                    os.link("{}/bcache2-benchmark/{}".format(server_dir, item),
                            "{}/{}".format(server_dir, bin_file))
                    found = True
                    break
            if not found:
                logging.error("not find benchmark bin file")
                return
            server_libs = server_dir + "/bcache2-benchmark/lib"
            os.makedirs(server_dir+"/lib")
            for file in os.listdir(server_libs):
                if file.endswith(".so"):
                    src = "{}/{}".format(server_libs, file)
                    dst = "{}/{}".format(server_dir, "lib/" + file)
                    logging.info("link local lib file, src {}, dst {}".format(src, dst))
                    os.link(src, dst)
            shutil.rmtree("{}/bcache2-benchmark".format(server_dir), ignore_errors=True)
        else:
            src = "{}/{}".format(conf.CURRENT_DIR, conf.BENCHMARK_LOCAL_BIN)
            dst = "{}/{}".format(server_dir, bin_file)
            logging.info("link local benchmark bin file, src {}, dst {}".format(src, dst))
            os.link(src, dst)
            server_libs = conf.CURRENT_DIR+"/output/lib"
            os.makedirs(server_dir+"/lib")
            for file in os.listdir(server_libs):
                if file.endswith(".so"):
                    src = "{}/{}".format(conf.CURRENT_DIR, "output/lib/" + file)
                    dst = "{}/{}".format(server_dir, "lib/" + file)
                    logging.info("link local lib file, src {}, dst {}".format(src, dst))
                    os.link(src, dst)
        flags = ' '.join(['--{}={}'.format(flag, value) for flag, value in flags.items()])
        table_uri = ""
        if ":" in ms_addr:
            table_uri += "tcp://{}".format(ms_addr)
        else:
            table_uri += "consul://{}".format(ms_addr)
        table_uri += "/{}/{}".format(namespace, table_name)
        flags += " --bench_bcache2_client_table_uri=" + table_uri
        logging.info("start benchmark, flag {}".format(flags))
        with open(server_dir + "/start.sh", "w") as fd:
            fd.write(
                """timeout {timeout} ./{bin} --bench_run_time=0 --bench_port={port} {flags} >stdout 2>stderr &""".
                format(bin=bin_file,
                       flags=flags,
                       port=port,
                       timeout=conf.PROC_TIMEOUT))
            os.chmod(server_dir + "/start.sh", 0o755)
        commands.run_shell("cd {} && ./start.sh".format(server_dir))
        assert common.check_with_retry(check_helper.check_port_used, conf.HOST_IP, port, interval=2)

    @staticmethod
    def stop_server(ip, port, service_name=""):
        logging.info("[Instance stop] {}, server_{}".format(conf.METASERVER_CLUSTER_NAME, port))
        commands.kill_process_by_name("server_{}".format(port), 'USR1')
        assert common.check_with_retry(check_helper.check_server_dead, ip, port)
        return True

    @staticmethod
    def hang_server(ip, port, service_name="", duration_s=0):
        logging.info("[Instance hang] {}, server_{}".format(conf.METASERVER_CLUSTER_NAME, port))
        commands.kill_process_by_name("server_{}".format(port), 'STOP')
        time.sleep(duration_s)
        commands.kill_process_by_name("server_{}".format(port), 'CONT')
        return True

    @staticmethod
    def start_server(ip, port):
        logging.info("start server {}".format(port))
        bin_file = "server_{}".format(port)
        server_dir = "{}/{}".format(conf.ONEBOX_DIR, bin_file)
        commands.run_shell("cd {} && ./start.sh".format(server_dir))
        assert common.check_with_retry(check_helper.check_server_ready, ip, port)

    @staticmethod
    def restart_server(ip, port, service_name="", stop_s=0):
        logging.info("restart server {}:{}".format(ip, port))
        LocalDeployer.stop_server(ip, port)
        time.sleep(stop_s)
        LocalDeployer.start_server(ip, port)
        return True

    @staticmethod
    def network_reject(target_ip, dst, probability, duration_s):
        logging.warn("not support")
        return False

    @staticmethod
    def network_drop(target_ip, dst, probability, duration_s):
        logging.warn("not support")
        return False

    @staticmethod
    def network_delay(target_ip, dst, delay_ms, duration_s):
        logging.warn("not support")
        return False

    @staticmethod
    def inject_code_fault(ip, port, fault_name, probability=100, duration_s=60):
        if not LocalDeployer.enable_fault(ip, port, fault_name, probability):
            return False
        time.sleep(duration_s)
        if not LocalDeployer.disable_fault(ip, port, fault_name):
            return False
        if check_helper.check_server_dead(ip, port):
            LocalDeployer.start_server(ip, port)
        return True

    @staticmethod
    def enable_fault(ip, port, fault_name, probability=100):
        return LocalDeployer.__send_fiu_control(ip, port, "enable_random name={},probability={}".format(
            fault_name, probability / 100.0))

    @staticmethod
    def disable_fault(ip, port, fault_name):
        return LocalDeployer.__send_fiu_control(ip, port, "disable name={}".format(fault_name))

    @staticmethod
    def __send_fiu_control(ip, port, control_command):
        server_name = "server_{}".format(port)
        output = commands.run_shell("{}/fiu-ctrl -c '{}' $(pgrep {})"
            .format(conf.TOOLS_DIR , control_command, server_name)).strip()
        logging.info(
            "Send FIU control command: ip={}, port={}, command={}, output={}".format(
                ip, port, control_command, output))
        return output == b'0' or output == b''


class RemoteDeployer():
    @staticmethod
    def setup():
        logging.info("kill the old onebox processes")
        for process_dir in os.listdir(conf.ONEBOX_DIR) if os.path.exists(conf.ONEBOX_DIR) else []:
            commands.kill_process_by_name(process_dir)

        logging.info("check wukong agent")
        assert conf.FAULT_INJECTER.check_wukong_agent()

    @staticmethod
    def teardown():
        logging.info("kill the old onebox processes")
        for process_dir in os.listdir(conf.ONEBOX_DIR) if os.path.exists(conf.ONEBOX_DIR) else []:
            commands.kill_process_by_name(process_dir)

    @staticmethod
    def stop_server(ip, port, service_name):
        logging.info("stop server {}:{}".format(ip, port))
        client = wukong.ByteChaosClient()
        prefix = 'XDG_RUNTIME_DIR="/run/user/$UID" DBUS_SESSION_BUS_ADDRESS="unix:path=${XDG_RUNTIME_DIR}/bus"'
        cmd = prefix + ' systemctl --user kill -s SIGKILL {}'.format(service_name)
        stop_command = 'su {} -c \'{}\''.format("tiger", cmd)
        execution_id = client.command_task(ip, stop_command)
        if execution_id == None:
            return False

        logging.info("waiting for server stop")
        if not common.check_with_retry(check_helper.check_server_dead, ip, port, interval=1, max_try=60):
            client.delete_task(execution_id)
            return False

        logging.info("waiting for task finish")

        while client.get_task_status(execution_id) == "running":
            time.sleep(1)
        client.delete_task(execution_id)

    @staticmethod
    def hang_server(ip, port, service_name, duration_s):
        logging.info("hang server {}:{}".format(ip, port))
        client = wukong.ByteChaosClient()
        prefix = 'XDG_RUNTIME_DIR="/run/user/$UID" DBUS_SESSION_BUS_ADDRESS="unix:path=${XDG_RUNTIME_DIR}/bus"'
        start_cmd = prefix + ' systemctl --user kill -s SIGSTOP {}'.format(service_name)
        stop_cmd = prefix + ' systemctl --user kill -s SIGCONT {}'.format(service_name)

        start_command = 'su {} -c \'{}\''.format("tiger", start_cmd)
        stop_command = 'su {} -c \'{}\''.format("tiger", stop_cmd)
        execution_id = client.command_task(ip, start_command, stop_command, duration_s)
        if execution_id == None:
            return False
        logging.info("waiting for task finish")
        while client.get_task_status(execution_id) == "running":
            time.sleep(1)
        client.delete_task(execution_id)
        return True

    @staticmethod
    def restart_server(ip, port, service_name, stop_s=0):
        logging.info("restart server {}:{}".format(ip, port))
        client = wukong.ByteChaosClient()
        prefix = 'XDG_RUNTIME_DIR="/run/user/$UID" DBUS_SESSION_BUS_ADDRESS="unix:path=${XDG_RUNTIME_DIR}/bus"'
        cmd = prefix + ' systemctl --user kill -s SIGKILL {}'.format(service_name)
        stop_command = 'su {} -c \'{}\''.format("tiger", cmd)
        execution_id = client.command_task(ip, stop_command)
        if execution_id == None:
            return False

        logging.info("waiting for server stop")
        if not common.check_with_retry(check_helper.check_server_dead, ip, port, interval=1, max_try=60):
            client.delete_task(execution_id)
            return False

        logging.info("waiting for task finish")
        while client.get_task_status(execution_id) == "running":
            time.sleep(1)
        client.delete_task(execution_id)

        logging.info("waiting for server start")
        if not common.check_with_retry(check_helper.check_server_ready, ip, port, interval=1, max_try=60):
            return False

        return True

    @staticmethod
    def network_reject(target_ip, reject_ip_list, probability, duration_s):
        reject_ip_list = list(set(map(lambda ip: ip+"/32", reject_ip_list)))
        logging.info("network reject, target_ip {}, reject_ip_list {}, probability {}, duration_s {}".format(
            target_ip, reject_ip_list, probability, duration_s))

        client = wukong.ByteChaosClient()
        execution_id = client.network_reject_task(target_ip, reject_ip_list, "cidr", probability, "tcp", duration_s)
        if execution_id == None:
            return False

        logging.info("waiting for task finish")
        while client.get_task_status(execution_id) == "running":
            time.sleep(1)
        client.delete_task(execution_id)
        return True

    @staticmethod
    def network_drop(target_ip, drop_ip_list, probability, duration_s):
        drop_ip_list = list(set(map(lambda ip: ip+"/32", drop_ip_list)))
        logging.info("network drop, target_ip {}, drop_ip_list {}, probability {}, duration_s {}".format(
            target_ip, drop_ip_list, probability, duration_s))

        client = wukong.ByteChaosClient()
        execution_id = client.network_drop_task(target_ip, drop_ip_list, "cidr", probability, "tcp", duration_s)
        if execution_id == None:
            return False

        logging.info("waiting for task finish")
        while client.get_task_status(execution_id) == "running":
            time.sleep(1)
        client.delete_task(execution_id)
        return True

    @staticmethod
    def network_delay(target_ip, delay_ip_list, delay_ms, duration_s):
        delay_ip_list = list(set(map(lambda ip: ip+"/32", delay_ip_list)))
        logging.info("network drop, target_ip {}, delay_ip_list {}, delay_ms {}, duration_s {}".format(
            target_ip, delay_ip_list, delay_ms, duration_s))

        client = wukong.ByteChaosClient()
        execution_id = client.network_delay_task(target_ip, delay_ip_list, "cidr", delay_ms, "tcp", duration_s)
        if execution_id == None:
            return False

        logging.info("waiting for task finish")
        while client.get_task_status(execution_id) == "running":
            time.sleep(1)
        client.delete_task(execution_id)
        return True

    @staticmethod
    def inject_code_fault(ip, port, fault_name, probability, duration_s):
        logging.info("inject code fault({}) to {}:{}".format(fault_name, ip, port))
        client = wukong.ByteChaosClient()
        process_pattern = "{}_{}".format(ip, port) # match the start cmd of server
        execution_id = client.code_fault_inject(ip, fault_name, probability, process_pattern, duration_s)
        if execution_id == None:
            return False

        logging.info("waiting for task finish")
        while client.get_task_status(execution_id) == "running":
            #  avoid chaos openapi flow control
            time.sleep(3)
        client.delete_task(execution_id)
        return True

    @staticmethod
    def disk_busy(ip, file_path, io_type, duration_s):
        client = wukong.ByteChaosClient()
        execution_id = client.disk_busy(ip, file_path, io_type, duration_s)
        if execution_id == None:
            return False

        while client.get_task_status(execution_id) == "running":
            time.sleep(1)
        client.delete_task(execution_id)
        return True

    @staticmethod
    def time_skew(ip, offset, duration_s):
        client = wukong.ByteChaosClient()
        execution_id = client.time_skew(ip, offset, duration_s)
        if execution_id == None:
            return False

        while client.get_task_status(execution_id) == "running":
            time.sleep(1)
        client.delete_task(execution_id)
        return True
