#!/usr/bin/env python
# coding=utf8
from tablestore import *
from mq_http_sdk.mq_exception import MQExceptionBase
from mq_http_sdk.mq_consumer import *
from mq_http_sdk.mq_client import *
from config import conf
import pymysql
import datetime
import numpy as np
from pandas.tseries.offsets import Day
from orm import mysql_ORM

class Rock_Consume:
    def __init__(self):
        #初始化 client
        rocket_str='ROCKETMQ'
        self.mq_client = MQClient(
            #设置HTTP接入域名（此处以公共云生产环境为例）
            conf.get(rocket_str,'HTTP_ENDPOINT'),
            #AccessKey 阿里云身份验证，在阿里云服务器管理控制台创建
            conf.get(rocket_str,'ACCESS_KEY'),
            #SecretKey 阿里云身份验证，在阿里云服务器管理控制台创建
            conf.get(rocket_str,'SECRET_KEY')
        )
        #所属的 Topic
        self.topic_name = conf.get(rocket_str,'TOPIC')
        #您在控制台创建的 Consumer ID(Group ID)
        self.group_id = conf.get(rocket_str,'GROUP_ID')
        #Topic所属实例ID，默认实例为空None
        self.instance_id = conf.get(rocket_str,'INSTANCE_ID')

        self.consumer = self.mq_client.get_consumer(self.instance_id, self.topic_name, self.group_id)

    def orm(self):
        pass

    def rollpoll(self):
        param_str='PARAM'
        #长轮询表示如果topic没有消息则请求会在服务端挂住3s，3s内如果有消息可以消费则立即返回
        #长轮询时间3秒（最多可设置为30秒）
        wait_seconds = int(conf.get(param_str,'wait_seconds'))
        #一次最多消费3条(最多可设置为16条)
        batch = int(conf.get(param_str,'batch'))
        print("%sConsume And Ak Message From Topic%s\nTopicName:%s\nMQConsumer:%s\nWaitSeconds:%s\n" % (10 * "=", 10 * "=", self.topic_name, self.group_id, wait_seconds))
        while True:
            try:
                #长轮询消费消息
                recv_msgs = self.consumer.consume_message(batch, wait_seconds)
                for msg in recv_msgs:
                    self.content_handler(msg)

            except MQExceptionBase as e:
                if e.type == "MessageNotExist":
                    print("No new message! RequestId: %s" % e.req_id)
                    time.sleep(3)
                    continue

                print("Consume Message Fail! Exception:%s\n" % e)
                time.sleep(3)
                continue

            #msg.next_consume_time前若不确认消息消费成功，则消息会重复消费
            #消息句柄有时间戳，同一条消息每次消费拿到的都不一样
            try:
                receipt_handle_list = [msg.receipt_handle for msg in recv_msgs]
                self.consumer.ack_message(receipt_handle_list)
                print("Ak %s Message Succeed.\n\n" % len(receipt_handle_list))
            except MQExceptionBase as e:
                print("\nAk Message Fail! Exception:%s" % e)
                #某些消息的句柄可能超时了会导致确认不成功
                '''
                if e.sub_errors:
                    for sub_error in e.sub_errors:
                        print "\tErrorHandle:%s,ErrorCode:%s,ErrorMsg:%s" % (sub_error["ReceiptHandle"], sub_error["ErrorCode"], sub_error["ErrorMessage"])
                '''

    def content_handler(self,msg):
        #清洗数据
        #处理，标记入库
        database='DATABASE'
        mysql_obj1=mysql_ORM(conf.get(database,'outer_server'),conf.get(database,'user'),conf.get(database,'pwd'),int(conf.get(database,'port')),"businessdata")
        mysql_conn1=mysql_obj1.connect_fc()
        db_sql1='insert into chatdata_classify(date,content,msgType,owner_wxid,realChatUsr,fromUsr,toUsr,repeat_num,account,classify) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        mysql_obj1.mult_add(mysql_conn1,[[msg.message_id,msg.message_body_md5,msg.message_tag, msg.consumed_times,msg.publish_time,msg.message_body,msg.next_consume_time,msg.receipt_handle,'111','111']],db_sql1)#
        print("Receive, MessageId: %s\nMessageBodyMD5: %s \
                                    MessageTag: %s\nConsumedTimes: %s \
                                    PublishTime: %s\nBody: %s \
                                    NextConsumeTime: %s \
                                    ReceiptHandle: %s" % \
                                    (msg.message_id, msg.message_body_md5,
                                    msg.message_tag, msg.consumed_times,
                                    msg.publish_time, msg.message_body,
                                    msg.next_consume_time, msg.receipt_handle))
        time.sleep(1)


if __name__ == "__main__":
    consumer=Rock_Consume()
    consumer.rollpoll()