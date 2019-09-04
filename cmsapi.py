import time

from tools import Prpcrypt


def get_token():
    now = int(time.time() * 1000)
    # print(type(now))
    phone = "18513290987"
    requsetkey = "jj3Q1h0H86FZ7CD46Z5Nr35p67L"
    secret_args = {"requestKey": requsetkey, "phone": phone, "systemId": "lab", "timeStamp": now}

    args_str = "{}-weidiango-{}-weidiango-{}-weidiango-{}".format(secret_args["phone"], secret_args["requestKey"],
                                                                  secret_args["systemId"], secret_args["timeStamp"])

    iv = "p6VueLBoQzBFxqgy"
    key = "76fzqD63cCmf39VI"

    pc = Prpcrypt(key, iv)
    token = pc.encrypt(args_str)

    return token.decode("utf-8")
