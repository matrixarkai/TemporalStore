import json
import logging
import requests


class Lark(object):
    SCHEME = "https"
    HOST = "open.feishu.cn"

    def __init__(self, app_id, app_secret):
        self.app_id = app_id
        self.app_secret = app_secret

    def __update_lark_token(self):
        URI = "/open-apis/auth/v3/tenant_access_token/internal/"
        resp = requests.post(
            url="{}://{}{}".format(self.SCHEME, self.HOST, URI),
            headers={
                "Content-Type": "application/json",
            },
            json={
                "app_id": self.app_id,
                "app_secret": self.app_secret,
            }
        )
        if resp.status_code != 200:
            return None
        return resp.json().get("tenant_access_token", None)

    def send_chat(self, chat_id, text):
        URI = "/open-apis/im/v1/messages"
        tenant_access_token = self.__update_lark_token()
        if tenant_access_token is None:
            logging.error("failed to get tenant_access_token")
            return False

        resp = requests.post(
            url="{}://{}{}".format(self.SCHEME, self.HOST, URI),
            headers={
                "Authorization": "Bearer {}".format(tenant_access_token),
                "Content-Type": "application/json",
            },
            params={
                "receive_id_type": "chat_id",
            },
            json={
                "receive_id": chat_id,
                "msg_type": "text",
                "content": text,
            }
        )
        return resp.json()

    def send_card(self, chat_id, card_msg):
        URI = "/open-apis/im/v1/messages"
        tenant_access_token = self.__update_lark_token()
        if tenant_access_token is None:
            logging.error("failed to get tenant_access_token")
            return False

        resp = requests.post(
            url="{}://{}{}".format(self.SCHEME, self.HOST, URI),
            headers={
                "Authorization": "Bearer {}".format(tenant_access_token),
                "Content-Type": "application/json",
            },
            params={
                "receive_id_type": "chat_id",
            },
            json={
                "receive_id": chat_id,
                "msg_type": "interactive",
                "content": card_msg,
            }
        )
        return resp.json()

    def send_chaos_template(self, chat_id, template_id, chaos_data: str, cluster_config: str, cluster_metrics: str):
        URI = "/open-apis/im/v1/messages"
        tenant_access_token = self.__update_lark_token()
        print(tenant_access_token)
        if tenant_access_token is None:
            logging.error("failed to get tenant_access_token")
            return False

        card = {
            "type": "template",
            "data": {
                "template_id": template_id,
                "template_variable":
                {
                    "chaos_data": chaos_data,
                    "cluster_config": cluster_config,
                    "cluster_metrics": cluster_metrics,
                }
            }
        }

        resp = requests.post(
            url="{}://{}{}".format(self.SCHEME, self.HOST, URI),
            headers={
                "Authorization": "Bearer {}".format(tenant_access_token),
                "Content-Type": "application/json",
            },
            params={
                "receive_id_type": "chat_id",
            },
            json={
                "receive_id": chat_id,
                "msg_type": "interactive",
                "content": json.dumps(card),
            }
        )
        print(resp.request.path_url)
        print(resp.request.body)
        return resp.json()


if __name__ == "__main__":
    lark = Lark("cli_a268df36bffa900d", "Oh0xPFaHVdAcrQiUlHFHqcF3zSNRqAMI")
    print(lark.send_chaos_template("oc_c2cb33eb3774d631c1fc6a085d9e620d", "ctp_AAuL2af7gGrQ", "**test markdown**", "", ""))
