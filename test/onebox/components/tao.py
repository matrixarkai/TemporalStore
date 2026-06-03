import time
import logging

from taopaas.report import ReportViewManager
from taopaas.task import ModuleTaskViewManager
from taopaas.base.client import TaoPaaSClient
from taopaas.base.exceptions import ResponseError, TaoError
from taopaas.base.client import AuthHeader


class Client():
    def __init__(self, token):
        auth_header = AuthHeader(token, typ="secret", env="online")
        self.client = TaoPaaSClient(auth_header, base_uri="https://tao.byted.org/")
        self.manager = ModuleTaskViewManager(client=self.client)

    def execute_command(self, node_id, command, ip_list=[], timeout=180):
        logging.debug("execute command {} on machines {} in node {}".format(command, ip_list, node_id))

        host_selectors = [{"selector": "node", "node_id": node_id}]
        if len(ip_list) != 0:
            host_selectors = [{"selector": "ip", "node_ids": [node_id], "ips": ip_list}]

        cmd = [
            {
                "module": "bash",
                "run_as": "tiger",
                "version": "latest",
                "args": command.split()
            }
        ]

        control = {
            "timeout": timeout,
            "pause_point": {"typ": "percent", "value": 100},
            "batch_size": {"typ": "percent", "value": 100},
        }

        try:
            task = self.manager.create(
                "[Chaos Test] {}".format(command),
                host_selectors,
                control,
                cmd,
                auto_start=True,
            )
        except ResponseError as e:
            logging.error("[{}]{}: {}".format(e.rid, e.status_code, e.msg))
            raise e
        except TaoError as e:
            raise e

        task_id = task.data.get("id")
        logging.debug("execute command {} on machines {} in node {}, task_id is {}".format(
            command, ip_list, node_id, task_id))
        self.manager.until_task_finish(task_id)

        report_manager = ReportViewManager(client=self.client)
        page = 1
        page_size = 1000
        success = True
        while True:
            report = report_manager.list(task_id, page=page, page_size=page_size)
            for item in report.data:
                if item["status"] != "success":
                    logging.error("tao execute command fails, host: {}, command=`{}` stdout: {}, stderr: {}".format(
                        item['host'], command, item['report'].get('stdout', "NULL"), item['report'].get('stderr', "NULL")))
                    success = False
                else:
                    logging.debug("task {} success".format(task_id))
            if len(report.data) < page_size:
                break
            time.sleep(0.5)
            page += 1

        return success

if __name__ == "__main__":
    client = Client("58a70817003101f7a02f529049a04e49")
    assert client.execute_command(2634291, "touch /tmp/test", "10.26.62.130")
