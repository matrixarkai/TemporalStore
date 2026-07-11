from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "sdk" / "python"))

from temporalstore.proxy_client import ProxyClient, ProxyOptions


class RecordingProxyClient(ProxyClient):
    def __init__(self):
        super().__init__(ProxyOptions(endpoint="http://127.0.0.1:17000", namespace_name="ns", table_name="tbl"))
        self.posts = []

    def _post(self, path, body):
        self.posts.append((path, body))
        return {"ok": True}


class ProxyClientBatchHsetTest(unittest.TestCase):
    def test_batch_hset_uses_single_hmset_for_same_key(self):
        client = RecordingProxyClient()

        client.batch_hset(
            [
                {"key": "k", "field": "f1", "value": "v1"},
                {"key": "k", "field": "f2", "value": "v2"},
            ]
        )

        self.assertEqual([path for path, _ in client.posts], ["/ProxyService/HMSet"])
        body = client.posts[0][1]
        self.assertEqual(body["key"], "k")
        self.assertEqual(body["entries"], [["f1", [118, 49]], ["f2", [118, 50]]])

    def test_batch_hset_groups_by_key(self):
        client = RecordingProxyClient()

        client.batch_hset(
            [
                {"key": "k1", "field": "a", "value": "1"},
                {"key": "k2", "field": "b", "value": "2"},
                {"key": "k1", "field": "c", "value": "3"},
            ]
        )

        self.assertEqual([path for path, _ in client.posts], ["/ProxyService/HMSet", "/ProxyService/HMSet"])
        self.assertEqual(client.posts[0][1]["key"], "k1")
        self.assertEqual(client.posts[0][1]["entries"], [["a", [49]], ["c", [51]]])
        self.assertEqual(client.posts[1][1]["key"], "k2")
        self.assertEqual(client.posts[1][1]["entries"], [["b", [50]]])


if __name__ == "__main__":
    unittest.main()