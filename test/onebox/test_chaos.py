import os
import time
import shutil
import logging
import inspect
import datetime
import threading

from onebox import conf
from onebox import commands
from onebox import lark_report
from onebox.test_consistency import test_consistency


total_round = 0
success_round = 0
failed_round = 0


def reporter_loop():
    global total_round
    global success_round
    global failed_round

    # align last_report_time_sec as 10:00 a.m.
    now = datetime.datetime.now()
    target = now.replace(hour=10, minute=0, second=0, microsecond=0)
    if now < target:
        yesterday = now - datetime.timedelta(days=1)
        target = yesterday.replace(hour=10, minute=0, second=0, microsecond=0)
    last_report_time_sec = target.timestamp()
    next_report_time_sec = last_report_time_sec + 86400
    logging.info("last_report_time_sec {}, next_report_time_sec {}".format(last_report_time_sec, next_report_time_sec))

    while True:
        if time.time() > next_report_time_sec:
            try:
                cluster_config = lark_report.gen_cluster_config()
                cluster_metrics = lark_report.gen_cluster_metrics()
                client_metrics = lark_report.gen_client_metrics()
                client_metrics += "\nChaos运行轮次(过去24小时) total/success/failed: {}/{}/{}".format(
                    total_round, success_round, failed_round)
                client_metrics += "\nChaos每轮次操作数: {}".format(conf.BENCHMARK_FLAGS["bench_checker_max_operation_per_round"])
                client_metrics += "\n故障注入次数(过去24小时): {}".format(conf.FAULT_INJECTER.get_inject_num())
                client_metrics += "\n故障注入类型: {}".format(conf.FAULT_INJECTER.get_fault_types())
                logging.info(cluster_config)
                logging.info(cluster_metrics)
                logging.info(client_metrics)
                conf.LARK.send_chaos_template(conf.LARK_CHAT_ID, conf.LARK_TEMPLATE_ID,
                                              client_metrics, cluster_config, cluster_metrics)
                total_round = 0
                success_round = 0
                failed_round = 0
                conf.FAULT_INJECTER.clear_inject_num()
            except Exception as ex:
                logging.warning("report lark failed: {}".format(str(ex)))
                conf.LARK.send_chat(conf.LARK_CHAT_ID, "Generate template failed: {}".format(ex))
            else:
                logging.info("report lark success")
            finally:
                now = datetime.datetime.now()
                target = now.replace(hour=10, minute=0, second=0, microsecond=0)
                if now < target:
                    yesterday = now - datetime.timedelta(days=1)
                    target = yesterday.replace(hour=10, minute=0, second=0, microsecond=0)
                last_report_time_sec = target.timestamp()
                next_report_time_sec = last_report_time_sec + 86400
                logging.info("last_report_time_sec {}, next_report_time_sec {}".format(
                    last_report_time_sec, next_report_time_sec))
        time.sleep(120)


def test_chaos():
    if conf.DEPLOYER_TYPE == "local":
        return

    global total_round
    global success_round
    global failed_round

    threading.Thread(target=reporter_loop).start()
    while True:
        # remove subdirs
        if os.path.exists(conf.ONEBOX_DIR):
            for subdir in [d for d in os.listdir(conf.ONEBOX_DIR) if os.path.isdir(os.path.join(conf.ONEBOX_DIR, d))]:
                dirpath = os.path.join(conf.ONEBOX_DIR, subdir)
                modtime_sec = os.path.getmtime(dirpath)
                if time.time() - modtime_sec > 36 * 60 * 60:
                    # remove subdir that are more than 48 hours old
                    shutil.rmtree(dirpath, ignore_errors=True)

        # run cases
        members = inspect.getmembers(test_consistency, inspect.isfunction)
        members = sorted(members, key=lambda x: inspect.getsourcelines(x[1])[1])
        for name, symbol in members:
            if not name.startswith("test_"):
                continue
            logging.info("run {}".format(name))
            try:
                symbol()
            except Exception as ex:
                logging.warning("run {} failed: {}".format(name, str(ex)))
                total_round += 1
                failed_round += 1
            else:
                logging.info("run {} success".format(name))
                total_round += conf.BENCHMARK_ROUND
                success_round += conf.BENCHMARK_ROUND
            finally:
                logging.info("total {}, success {}, failed {}".format(total_round, success_round, failed_round))
                # kill all running process to run next case
                for process_dir in os.listdir(conf.ONEBOX_DIR) if os.path.exists(conf.ONEBOX_DIR) else []:
                    logging.info("kill {}".format(process_dir))
                    commands.kill_process_by_name(process_dir, 'INT')

        time.sleep(1)
