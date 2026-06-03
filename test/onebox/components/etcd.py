import http
import base64
import requests


class Client():
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.uri = "http://{}:{}/v3/kv".format(self.host, self.port)
        self.session = requests.session()

    def put(self, key, value):
        resp = self.session.post("{}/put".format(self.uri), json={
            "key": base64.b64encode(key.encode()).decode(),
            "value": base64.b64encode(value.encode()).decode(),
        })
        if resp.status_code != http.HTTPStatus.OK:
            raise Exception("invalid response: {}".format(resp.text))

    def get(self, key):
        resp = self.session.post("{}/range".format(self.uri), json={
            "key": base64.b64encode(key.encode()).decode(),
        })
        if resp.status_code != http.HTTPStatus.OK:
            raise Exception("invalid response: {}".format(resp.text))
        return base64.b64decode(resp.json()["kvs"][0]["value"].encode()).decode()


if __name__ == "__main__":
    client = Client("10.128.114.95", 1769)
    client.put("test", "vxxx")
    print(client.get("test"))
