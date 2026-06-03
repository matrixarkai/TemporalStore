import copy
import time
import json
import logging

from onebox import conf
from onebox import common
from onebox import commands
from onebox import deployer
from onebox.components import brpc_stub


def init_bench_flags(flags, idc_type):
    namespace = conf.NAMESPACE
    table = namespace['tables'][0]
    if idc_type == 'main':
        flags['bench_bcache2_client_pin_primary'] = True
        idc = table['partition_units'][0]['placement_set'][0]['vdc']
    else:
        flags['bench_bcache2_client_pin_primary'] = False
        idc = table['partition_units'][0]['placement_set'][1]['vdc']
    flags["bench_bcache2_client_idc"] = idc


def check_sla_satisfy(ip, port):
    client = brpc_stub.Client(ip, port)
    resp = client.call("BenchService", "GetStats")
    # logging.info("bench stats: {}".format(json.dumps(resp)))

    if resp.get("consistency", False) is False:
        logging.warn("consistency check failed")
        assert False

    if resp.get("checking", False) is True:
        logging.info("waiting for consistency check finish")
        return True

    qps = resp["total_stats"].get("total_qps", 0)
    success_qps = resp["total_stats"].get("success_qps", 0)
    avg_latency_us = resp["total_stats"].get("avg_latency_us", 0)
    p99_latency_us = resp["total_stats"].get("p99_latency_us", 0)
    availability = 0 if qps == 0 else success_qps / qps
    if qps < conf.SLA_TOTAL_QPS:
        logging.info("qps too low, qps {}, sla_total_qps {}".format(qps, conf.SLA_TOTAL_QPS))
        return False
    if availability < conf.SLA_TOTAL_AVAILABILITY:
        logging.info("availability too low, availability {}, sla_total_availability {}".format(
            availability, conf.SLA_TOTAL_AVAILABILITY))
        return False
    if avg_latency_us > conf.SLA_TOTAL_AVG_LATENCY_MS * 1000:
        logging.info("avg latency too high, avg_latency_us {}, sla_total_avg_latency_ms {}".format(
            avg_latency_us, conf.SLA_TOTAL_AVG_LATENCY_MS * 1000))
        return False
    if p99_latency_us > conf.SLA_TOTAL_P99_LATENCY_MS * 1000:
        logging.info("p99 latency too high, p99_latency_us{}, sla_total_p99_latency_ms {}".format(
            p99_latency_us, conf.SLA_TOTAL_P99_LATENCY_MS * 1000))
        return False

    for command_stat in resp["command_stats"]:
        qps = command_stat.get("total_qps", 0)
        success_qps = command_stat.get("success_qps", 0)
        avg_latency_us = command_stat.get("avg_latency_us", 0)
        p99_latency_us = command_stat.get("p99_latency_us", 0)
        availability = 0 if qps == 0 else success_qps / qps
        for command_sla in conf.SLA_COMMAND:
            if command_stat["command"] == command_sla["NAME"]:
                if qps < command_sla["QPS"]:
                    logging.info("qps too low, qps {}, sla_total_qps {}".format(qps, command_sla["QPS"]))
                    return False
                if availability < command_sla["AVAILABILITY"]:
                    logging.info("availability too low, availability {}, sla_total_availability {}".format(
                        availability, command_sla["AVAILABILITY"]))
                    return False
                if avg_latency_us > command_sla["AVG_LATENCY_MS"] * 1000:
                    logging.info("avg latency too high, avg_latency_us {}, sla_total_avg_latency_ms {}".format(
                        avg_latency_us, command_sla["AVG_LATENCY_MS"] * 1000))
                    return False
                if p99_latency_us > command_sla["P99_LATENCY_MS"] * 1000:
                    logging.info("p99 latency too high, p99_latency_us{}, sla_total_p99_latency_ms {}".format(
                        p99_latency_us, command_sla["P99_LATENCY_MS"] * 1000))
                    return False
    logging.debug("check sla ok")
    return True


def run_check_loop(benchmark_name, benchmark_port):
    while True:
        client = brpc_stub.Client(conf.HOST_IP, benchmark_port)
        resp = client.call("BenchService", "GetStats")
        if resp.get("round", 0) == conf.BENCHMARK_ROUND:
            commands.kill_process_by_name(benchmark_name, "INT")
            break

        if conf.DEPLOYER_TYPE == "local":
            start = int(time.time() * 1000)
            assert common.check_with_retry(check_sla_satisfy, conf.HOST_IP, benchmark_port, max_try=20, interval=3)
            end = int(time.time() * 1000)
            # logging.info("check sla ok : duration {}ms, start {}, end {}".format(end - start, start, end))

        time.sleep(1)


def test_string_linearizability():
    benchmark_port = common.get_unused_port() if conf.BENCHMARK_PORT == 0 else conf.BENCHMARK_PORT
    benchmark_flags = copy.deepcopy(conf.BENCHMARK_FLAGS)
    init_bench_flags(benchmark_flags, 'main')
    benchmark_flags["bench_common_workload_freq"] = 1
    benchmark_flags["bench_string_workload_freq"] = 10
    benchmark_flags["bench_hash_workload_freq"] = 0
    benchmark_flags["bench_checker_eventual_consistency_mode"] = False
    namespace = conf.NAMESPACE
    table = namespace['tables'][0]['name']
    benchmark_name = "bench_string_linearizability_{}".format(int(time.time()))
    logging.info("setup benchmark on port {}".format(benchmark_port))
    deployer.LocalDeployer.setup_benchmark(namespace["name"], table, benchmark_name, benchmark_port, benchmark_flags)

    run_check_loop(benchmark_name, benchmark_port)


def test_hash_linearizability():
    benchmark_port = common.get_unused_port() if conf.BENCHMARK_PORT == 0 else conf.BENCHMARK_PORT
    benchmark_flags = copy.deepcopy(conf.BENCHMARK_FLAGS)
    init_bench_flags(benchmark_flags, 'main')
    benchmark_flags["bench_common_workload_freq"] = 1
    benchmark_flags["bench_string_workload_freq"] = 0
    benchmark_flags["bench_hash_workload_freq"] = 10
    benchmark_flags["bench_checker_eventual_consistency_mode"] = False
    namespace = conf.NAMESPACE
    table = namespace['tables'][0]['name']
    benchmark_name = "bench_hash_linearizability_{}".format(int(time.time()))
    logging.info("setup benchmark on port {}".format(benchmark_port))
    deployer.LocalDeployer.setup_benchmark(namespace["name"], table, benchmark_name, benchmark_port, benchmark_flags)

    run_check_loop(benchmark_name, benchmark_port)


def test_string_eventual():
    benchmark_port = common.get_unused_port() if conf.BENCHMARK_PORT == 0 else conf.BENCHMARK_PORT
    benchmark_flags = copy.deepcopy(conf.BENCHMARK_FLAGS)
    init_bench_flags(benchmark_flags, 'sub')
    benchmark_flags["bench_common_workload_freq"] = 1
    benchmark_flags["bench_string_workload_freq"] = 10
    benchmark_flags["bench_hash_workload_freq"] = 0
    benchmark_flags["bench_checker_eventual_consistency_mode"] = True
    namespace = conf.NAMESPACE
    table = namespace['tables'][0]['name']
    benchmark_name = "bench_string_eventual_{}".format(int(time.time()))
    logging.info("setup benchmark on port {}".format(benchmark_port))
    deployer.LocalDeployer.setup_benchmark(namespace["name"], table, benchmark_name, benchmark_port, benchmark_flags)

    run_check_loop(benchmark_name, benchmark_port)


def test_hash_eventual():
    benchmark_port = common.get_unused_port() if conf.BENCHMARK_PORT == 0 else conf.BENCHMARK_PORT
    benchmark_flags = copy.deepcopy(conf.BENCHMARK_FLAGS)
    init_bench_flags(benchmark_flags, 'sub')
    benchmark_flags["bench_common_workload_freq"] = 1
    benchmark_flags["bench_string_workload_freq"] = 0
    benchmark_flags["bench_hash_workload_freq"] = 10
    benchmark_flags["bench_checker_eventual_consistency_mode"] = True
    namespace = conf.NAMESPACE
    table = namespace['tables'][0]['name']
    benchmark_name = "bench_hash_eventual_{}".format(int(time.time()))
    logging.info("setup benchmark on port {}".format(benchmark_port))
    deployer.LocalDeployer.setup_benchmark(namespace["name"], table, benchmark_name, benchmark_port, benchmark_flags)

    run_check_loop(benchmark_name, benchmark_port)
