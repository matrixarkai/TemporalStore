import logging

import requests
from requests.adapters import HTTPAdapter, Retry


class Client(object):
    def __init__(self, host, port):
        retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
        self.session = requests.session()
        self.session.mount("http://", HTTPAdapter(max_retries=retries))
        self.host = host
        self.port = port

    def call(self, service, method, request_json: dict = {}):
        resp = self.session.post(
            url="http://{}:{}/{}/{}".format(self.host, self.port, service, method),
            json=request_json,
            timeout=30,
        )
        logging.debug("http request {}, body {}".format(resp.request.url, resp.request.body.decode()[:10240]))
        logging.debug("http response body {}".format(resp.text[:10240]))
        return resp.json()
