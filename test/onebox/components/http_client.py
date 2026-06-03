import time
import sys
import logging
import requests


class Client():
    def __init__(self):
        self.session = requests.session()

    def get(self, url, params={}, *, retry=sys.maxsize, interval_ms=100):
        for _ in range(retry):
            resp = self.session.get(url, params=params)
            logging.debug("http request {}".format(resp.request.url))
            logging.debug("http response body {}".format(resp.text[:10240]))
            if resp.status_code == 200:
                return resp
            time.sleep(interval_ms / 1000)
            interval_ms = min(interval_ms*2, 3000)

    def put(self, url, json={}, *, retry=sys.maxsize, interval_ms=100):
        for _ in range(retry):
            resp = self.session.put(url, json=json)
            logging.debug("http request {} {}, body {}".format(resp.request.method,
                          resp.request.url, resp.request.body.decode()[:10240]))
            logging.debug("http response body {}".format(resp.text[:10240]))
            if resp.status_code == 200:
                return resp
            time.sleep(interval_ms / 1000)
            interval_ms = min(interval_ms*2, 3000)

    def delete(self, url, json={}, *, retry=sys.maxsize, interval_ms=100):
        for _ in range(retry):
            resp = self.session.delete(url, json=json)
            logging.debug("http request {} {}, body {}".format(resp.request.method,
                          resp.request.url, resp.request.body.decode()[:10240]))
            logging.debug("http response body {}".format(resp.text[:10240]))
            if resp.status_code == 200:
                return resp
            time.sleep(interval_ms / 1000)
            interval_ms = min(interval_ms*2, 3000)

    def post(self, url, json={}, *, retry=sys.maxsize, interval_ms=100):
        for _ in range(retry):
            resp = self.session.post(url, json=json)
            logging.debug("http request {} {}, body {}".format(resp.request.method,
                          resp.request.url, resp.request.body.decode()[:10240]))
            logging.debug("http response body {}".format(resp.text[:10240]))
            if resp.status_code == 200:
                return resp
            time.sleep(interval_ms / 1000)
            interval_ms = min(interval_ms*2, 3000)
