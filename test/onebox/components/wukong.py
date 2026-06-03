# coding: utf-8

import json
import random
import requests
import logging


SECRET = "18f61a0f74faab444575ab4ee7bd0fa8"

BOE_HOST = "https://paas-gw-boe.byted.org"
CN_HOST = "https://paas-gw.byted.org"

PSM = "bytedance.bcache2.chaos"
WORKSPACE = 101  # BCache工作空间


class ByteChaosClient(object):
    def __init__(self, host=CN_HOST, secret=SECRET, psm=PSM, workspace=WORKSPACE):
        self.host = host
        self.psm = psm
        self.workspace = workspace

        session = requests.Session()
        session.headers.update(
            {
                "Authorization": "Bearer {}".format(secret),
                "Domain": "Chaos",
                "Content-Type": "application/json",
            }
        )
        self.session = session

    def network_reject_task(
        self, target_ip, dst, dst_type, probability, proto, duration_s
    ):
        """
        Example:
            {
                "duration": 120,
                "kind": "wukonglab@physical_network_deny",
                "name": "job-04892",
                "params": {
                    "dst": [
                        "10.227.80.187:22"
                    ],
                    "dstType": "ip_port",
                    "probability": 100,
                    "proto": "tcp"
                }
            }
        """

        job_name = "{}_network_reject_{}".format(target_ip, random.randint(1, 10000))
        source_name = "source-{}-{}".format(self.psm, random.randint(1, 10000))

        job = {
            "duration": duration_s,
            "kind": "wukonglab@physical_network_deny",
            "name": job_name,
            "params": {
                "dst": dst,
                "dstType": dst_type,
                "probability": probability,
                "proto": proto,
            },
        }
        source = {
            "filter": "nodeIP",
            "mode": "fixedNum",
            "name": source_name,
            "nodeIPFilter": [
                target_ip,
            ],
            "psm": self.psm,
            "region": "China-North",
            "type": "physical_source",
            "value": 1,
        }
        scene = {
            "jobGroups": [job_name],
            "sceneName": "bytedance.bcache2.chaos",
            "source": source_name,
        }

        return self.create_and_start_task([job], [source], [scene])

    def network_drop_task(
        self, target_ip, dst, dst_type, probability, proto, duration_s
    ):
        """
        Example:
            {
                "duration": 120,
                "kind": "wukonglab@physical_network_drop",
                "name": "job-04892",
                "params": {
                    "dst": [
                        "10.227.80.187:22"
                    ],
                    "dstType": "ip_port",
                    "probability": 100,
                    "proto": "tcp"
                }
            }
        """

        job_name = "{}_network_drop_{}".format(target_ip, random.randint(1, 10000))
        source_name = "source-{}-{}".format(self.psm, random.randint(1, 10000))

        job = {
            "duration": duration_s,
            "kind": "wukonglab@physical_network_drop",
            "name": job_name,
            "params": {
                "dst": dst,
                "dstType": dst_type,
                "probability": probability,
                "proto": proto,
            },
        }
        source = {
            "filter": "nodeIP",
            "mode": "fixedNum",
            "name": source_name,
            "nodeIPFilter": [
                target_ip,
            ],
            "psm": self.psm,
            "region": "China-North",
            "type": "physical_source",
            "value": 1,
        }
        scene = {
            "jobGroups": [job_name],
            "sceneName": "bytedance.bcache2.chaos",
            "source": source_name,
        }

        return self.create_and_start_task([job], [source], [scene])

    def network_delay_task(self, target_ip, dst, dst_type, delay_ms, proto, duration_s):
        """
        Example:
            {
            "duration": 120,
            "kind": "wukonglab@physical_network_latency",
            "name": "未命名",
            "params": {
                "delayDuration": 100,
                "dst": [
                "10.227.80.187:22"
                ],
                "dstType": "ip_port",
                "proto": "tcp"
            }
            }
        """

        job_name = "{}_network_delay_{}".format(target_ip, random.randint(1, 10000))
        source_name = "source-{}-{}".format(self.psm, random.randint(1, 10000))

        job = {
            "duration": duration_s,
            "kind": "wukonglab@physical_network_latency",
            "name": job_name,
            "params": {
                "delayDuration": delay_ms,
                "dst": dst,
                "dstType": dst_type,
                "proto": proto,
            },
        }
        source = {
            "filter": "nodeIP",
            "mode": "fixedNum",
            "name": source_name,
            "nodeIPFilter": [
                target_ip,
            ],
            "psm": self.psm,
            "region": "China-North",
            "type": "physical_source",
            "value": 1,
        }
        scene = {
            "jobGroups": [job_name],
            "sceneName": "bytedance.bcache2.chaos",
            "source": source_name,
        }

        return self.create_and_start_task([job], [source], [scene])

    def command_task(self, target_ip, start_cmd, stop_cmd="date", wait_stop_s=3):
        """
        Example:
        {
            "duration": 120,
            "kind": "wukonglab@physical_custom_command",
            "name": "未命名",
            "params": {
                "startCmd": "ls /var",
                "stopCmd": "ls /"
            }
        }
        """

        job_name = "{}_command_{}".format(target_ip, random.randint(1, 10000))
        source_name = "source-{}-{}".format(self.psm, random.randint(1, 10000))

        job = {
            "duration": wait_stop_s,
            "kind": "wukonglab@physical_custom_command",
            "name": job_name,
            "params": {
                "startCmd": start_cmd,
                "stopCmd": stop_cmd,
            },
        }
        source = {
            "filter": "nodeIP",
            "mode": "fixedNum",
            "name": source_name,
            "nodeIPFilter": [
                target_ip,
            ],
            "psm": self.psm,
            "region": "China-North",
            "type": "physical_source",
            "value": 1,
        }
        scene = {
            "jobGroups": [job_name],
            "sceneName": "bytedance.bcache2.chaos",
            "source": source_name,
        }

        return self.create_and_start_task([job], [source], [scene])

    def code_fault_inject(self, target_ip, failure_point, probability, process_pattern, duration_s=3):
        job_name = "{}_code_fault_inject_{}".format(target_ip, random.randint(1, 10000))
        source_name = "source-{}-{}".format(self.psm, random.randint(1, 10000))

        job = {
            "duration": duration_s,
            "kind": "wukonglab@physical_code_fault_injection",
            "name": job_name,
            "params": {
                "failurePoint": failure_point,
                "probability": probability,
                "process": process_pattern,
            },
        }
        source = {
            "filter": "nodeIP",
            "mode": "fixedNum",
            "name": source_name,
            "nodeIPFilter": [
                target_ip,
            ],
            "psm": self.psm,
            "region": "China-North",
            "type": "physical_source",
            "value": 1,
        }
        scene = {
            "jobGroups": [job_name],
            "sceneName": "bytedance.bcache2.chaos",
            "source": source_name,
        }

        return self.create_and_start_task([job], [source], [scene])

    def disk_busy(self, target_ip, path, io_type, duration_s=3):
        job_name = "{}_disk_{}_busy_{}".format(target_ip, io_type, random.randint(1, 10000))
        source_name = "source-{}-{}".format(self.psm, random.randint(1, 10000))

        kind = "wukonglab@physical_disk_{}_busy".format(io_type)
        job = {
            "duration": duration_s,
            "kind": kind,
            "name": job_name,
            "params": {
                "path": path,
            },
        }
        source = {
            "filter": "nodeIP",
            "mode": "fixedNum",
            "name": source_name,
            "nodeIPFilter": [
                target_ip,
            ],
            "psm": self.psm,
            "region": "China-North",
            "type": "physical_source",
            "value": 1,
        }
        scene = {
            "jobGroups": [job_name],
            "sceneName": "bytedance.bcache2.chaos",
            "source": source_name,
        }

        return self.create_and_start_task([job], [source], [scene])

    def time_skew(self, target_ip, offset, duration_s=3):
        job_name = "{}_time_skew_{}".format(target_ip, random.randint(1, 10000))
        source_name = "source-{}-{}".format(self.psm, random.randint(1, 10000))

        job = {
            "duration": duration_s,
            "kind": "wukonglab@physical_timeskew",
            "name": job_name,
            "params": {
                "offset": offset,
            },
        }
        source = {
            "filter": "nodeIP",
            "mode": "fixedNum",
            "name": source_name,
            "nodeIPFilter": [
                target_ip,
            ],
            "psm": self.psm,
            "region": "China-North",
            "type": "physical_source",
            "value": 1,
        }
        scene = {
            "jobGroups": [job_name],
            "sceneName": "bytedance.bcache2.chaos",
            "source": source_name,
        }

        return self.create_and_start_task([job], [source], [scene])

    def process_http_resp(self, resp: requests.Response):
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            logging.error("http request failed, code:{}, reason:{}"
                          .format(e.response.status_code, e.response.reason))
            return None
        else:
            return resp.json()

    def create_process(self, jobs, sources, scenes):
        resp = self.session.post(
            "{}/chaos/api/v2/openapi/spaces/{}/processes".format(
                self.host, self.workspace
            ),
            data=json.dumps(
                {
                    "process": {
                        "jobs": jobs,
                        "sources": sources,
                        "scenes": scenes,
                    }
                }
            ),
        )
        return self.process_http_resp(resp)

    def get_task_status(self, execution_id): # -> running, done, failed
        """
        return values can be divided by
        1. normal but not finished, including: running, created, pending, suspended
        2. normal and finished: completed: completed
        3. abnormal need to be deleted: others
        """
        resp = self.session.get(
                "{}/chaos/api/v2/openapi/executes/get-result".format(
                    self.host, self.workspace
                ),
                params={
                    "id": execution_id,
                },
            )

        rsp = self.process_http_resp(resp)
        if rsp == None:
            return "failed"
        elif rsp["data"]["status"] in ["running", "created", "pending", "suspended"]:
            return "running"
        elif rsp["data"]["status"] == "completed":
            return "done"
        else:
            logging.error("process status abnormal, rsp:{}".format(rsp))
            return "failed"

    def delete_task(self, execution_id):
        resp = self.session.post(
                "{}/chaos/api/v2/openapi/executes/stop".format(
                    self.host, self.workspace
                ),
                data=json.dumps(
                    {
                        "id": execution_id,
                    }
                ),
        )
        return self.process_http_resp(resp)

    def start_task(self, process_id):
        resp = self.session.post(
            "{}/chaos/api/v2/openapi/executes/start".format(
                self.host, self.workspace
            ),
            data=json.dumps(
                {
                    "processId": process_id,
                }
            ),
        )
        return self.process_http_resp(resp)

    def create_and_start_task(self, jobs, sources, scenes):  # -> execution_id or None
        resp = self.create_process(jobs, sources, scenes)
        if resp == None or resp["code"] != 0:
            logging.error("create process failed, rsp:{}".format(resp))
            return None
        process_id = resp["data"]["id"]

        rsp = self.start_task(process_id)
        if rsp == None or rsp["code"] != 0:
            logging.error("start process failed, rsp:{}".format(rsp))
            return None

        return rsp["data"]["executeId"]

if __name__ == "__main__":
    client = ByteChaosClient()
    execution_id = client.network_reject_task(
            "10.23.46.198",
            ["1.1.1.1:22", "2.2.2.2:80"],
            "ip_port",
            100,
            "tcp",
            20,
    )
    logging.info("execution_id: {}".format(execution_id))

    status = client.get_task_status(execution_id)
    logging.info("status: {}".format(status))