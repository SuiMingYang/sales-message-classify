# coding: utf8
import base64
from Crypto.Cipher import AES
from binascii import b2a_hex, a2b_hex, b2a_base64


class Prpcrypt():
    def __init__(self, key, iv):
        self.key = key.encode("utf-8")
        self.iv = iv.encode("utf-8")
        self.mode = AES.MODE_CBC

    # 加密函数，如果text不是16的倍数【加密文本text必须为16的倍数！】，那就补足为16的倍数

    def encrypt(self, text):

        # padding算法
        BS = 16  # aes数据分组长度为128 bit
        pad = lambda s: s + (BS - len(s) % BS) * chr(BS - len(s) % BS)
        text = pad(text).encode("utf-8")
        # print(text)
        cryptor = AES.new(self.key, self.mode, self.iv)

        self.ciphertext = cryptor.encrypt(text)
        # 因为AES加密时候得到的字符串不一定是ascii字符集的，输出到终端或者保存时候可能存在问题
        # 所以这里统一把加密后的字符串转化为16进制字符串 ,当然也可以转换为base64加密的内容，可以使用b2a_base64(self.ciphertext)
        return b2a_hex(self.ciphertext)

        # 解密函数
    def decrypt(self, text):
        # 此处的参数text 是传进来的经过加密后access_token 因此需要解密
        cryptor = AES.new(self.key, self.mode, self.iv)
        plain_text = cryptor.decrypt(a2b_hex(text))
        unpad = lambda s: s[0:-ord(s[-1])]
        return unpad(plain_text.decode("utf-8"))
