#coding:utf-8
import re
import jieba
import jieba.posseg as pseg # 词性标注
#from jieba.analyse import ChineseAnalyzer
import numpy as np
import pandas as pd
from jieba.analyse import extract_tags
import json
import pymysql
import datetime
from pandas.tseries.offsets import Day
from orm import mysql_ORM
class socailchat:
    def __init__(self):
        self.conn=pymysql.connect('rm-bp171b759ha99x5wfso.mysql.rds.aliyuncs.com',port=3306,user='root',passwd='HPGQEhutFBUCi8ZE8JYgWDwZVhAHXWJx',db='businessdata')

    def from_channel_get_group(self):
        """
        获取渠道，筛选我们的群
        """
        #result=pd.read_sql("select content,FROM_UNIXTIME(createTime) as createTime,fromUsr,msgType,owner_wxid,realChatUsr,toUsr from chatdata",conn)
        group_list=pd.read_sql("SELECT groupid as fromUsr,user as account from businessdata.channel_group;",self.conn)
        return group_list
    
    def progress(self,proname,msgType,rela_dict):
        try:
            seg_obj={}
            second_type=[]
            if proname=="":
                return "","","空白"
            if proname.find("@")>-1:
                if msgType=="TEXT":
                    if proname.find(u"所有人")>-1 or proname.find(u"所有成员")>-1:
                        return "","","广告"
                    elif proname.find(u"女王")>-1:
                        return "","","咨询"
                    else:
                        "","",""
                else:
                    pass
                    # if proname.find(u"weidiangou")>-1 or proname.find(u"shuidichou")>-1:
                    #     return "","","广告"
                    # else:
                    #     return "","",""
            else:
                pass

            if proname.find("<br/>")>-1:
                return "","","广告"
            
            proname=''.join(re.findall(u'[\d\u4e00-\u9fff]+', proname))#a-zA-Z
            seg_list=pseg.cut(proname)#,cut_all=False)
            '''
            #获取权重，后续计算分值用
            weight_arr=extract_tags(''.join(re.findall(u'[a-zA-Z\u4e00-\u9fff]+', proname)), topK=3, withWeight=True, allowPOS=())
            feature=""
            for item in weight_arr:
                feature+=item[0]
            '''
            #print(feature)
            #print(weight_arr)
            for word in seg_list:
                #if word.flag.find("n")>-1:
                #if word.flag=="n":
                try:
                    if rela_dict[word.word]!=None:
                        second_type.append(word.word+":")
                        second_type.append(rela_dict[word.word])
                except Exception:
                    pass

                if word.flag in list(seg_obj.keys()):
                    seg_obj[word.flag].append(word.word)
                else:
                    seg_obj[word.flag]=[]
                    seg_obj[word.flag].append(word.word)
                #else:
                #    pass
            #seg_arr.append(seg_obj)
            #second_arr.append(second_type)
            type_sum={}
            #计数
            for stype in second_type:
                if stype in list(type_sum.keys()):
                    type_sum[stype]+=1
                else:
                    type_sum[stype]=1
            target_name=""
            target_number=1
            #选择最多的二级品类
            for item in type_sum.keys():
                if type_sum[item]>=target_number:
                    target_name=item
                    target_number=type_sum[item]
                else:
                    pass
            return seg_obj,second_type,target_name#,feature
        except Exception:
            return "","",""#,""

if __name__ == "__main__":
    obj=socailchat()
    yesterday = (datetime.datetime.now() -1*Day()).strftime('%Y-%m-%d')#input(u'请输入结束时间：如（2019-01-07）')
    print(yesterday)
    #result=pd.read_sql("select content,FROM_UNIXTIME(createTime) as createTime,fromUsr,msgType,owner_wxid,realChatUsr,toUsr from chatdata",conn)
    data=pd.read_sql("SELECT substring_index(FROM_UNIXTIME(createTime),' ',1) as createTime,content,msgType,owner_wxid,realChatUsr,fromUsr,toUsr,count(1) as repeat_num from businessdata.chatdata where (msgType='WEB_PAGE' or msgType='IMAGE' or msgType='TEXT') and content not like (%s) and substring_index(FROM_UNIXTIME(createTime),' ',1)='%s' group by content,fromUsr,realChatUsr,owner_wxid;" % (u"'%为了给大家营造一个良好的购物%'",yesterday),obj.conn)
    clientsale=pd.read_sql("""select owner_wxid,fromUsr from businessdata.chatdata where substring_index(FROM_UNIXTIME(createTime),' ',1)='%s' group by owner_wxid,fromUsr;""" % yesterday,obj.conn)

    #先做群过滤
    groupid=obj.from_channel_get_group()
    data=data[data['fromUsr'].isin(groupid['fromUsr'])]
    data=pd.merge(data, groupid, on='fromUsr', how='left')
    obj.conn.close()
    #data=data.reindex(range(len(data)))

    #过滤出群内的客服
    clientsale=clientsale[clientsale['fromUsr'].isin(groupid['fromUsr'])]
    clientsale=list(set(clientsale['owner_wxid']))#去重复
    print("客服数",len(clientsale))

    commend_data=data[((data['msgType']=='IMAGE') | (data['msgType']=='WEB_PAGE')) & (data['realChatUsr'].isin(clientsale))]
    data=data[(data['msgType']=='TEXT') | (data['msgType']=='WEB_PAGE')]
    data=data.reset_index()
    #data=pd.read_csv(u'./群内消息/2019-03-21.csv')
    jieba.load_userdict("./sentiment/expendword.txt")

    # 分类做键值对字典
    relation=pd.read_csv(u'./sentiment/sentiment.csv')
    rela_group=relation.groupby([u'type',u'word'])
    rela_dict={}
    for g in rela_group:
        #print(g[0])
        rela_dict[g[0][1]]=g[0][0]
        #print(g[0][0],g[0][1])
    #print(rela_dict)

    #每个标题，按词性分组
    seg_arr=[]
    second_arr=[]
    target_arr=[]
    user_arr=[]
    
    print("****************start***************")
    for i,seg in enumerate(data[u'content']):
        seg_obj,second_type,target_name=obj.progress(seg,data[u'msgType'][i],rela_dict)
        seg_arr.append(seg_obj)
        second_arr.append(second_type)
        target_arr.append(target_name)
        #feature_arr.append(feature)
        #print('精确模式:','/'.join(seg_list))
    print("****************end***************")
    #print(seg_arr)
    #data.drop(columns=[u'content'],inplace=True)
    #data[u"三级分词"]=seg_arr
    #data[u"二级分词"]=second_arr
    data[u"类型"]=target_arr
    #data[u'特征']=feature_arr
    #data.to_csv('./result/data/%s_分类结果.csv' % yesterday)
    
    #data = pd.read_csv('./分类结果.csv')
    print("总数",len(data))

    efficient_data=data[(data[u"类型"]=="") | (data[u"类型"]==u"咨询") | (data[u"类型"]==u"空白") ]
    print("有效数",len(efficient_data))
    consult_data=data[data[u"类型"]==u"咨询"]
    print("咨询数",len(consult_data))
    remove_data=data[(data[u"类型"]!="") & (data[u"类型"]!=u"咨询") & (data[u"类型"]!=u"空白") ]
    print("垃圾数",len(remove_data))
    remove_data.to_csv('./result/data/%s_垃圾结果.csv' % yesterday)
    empty_data=data[data[u"类型"]==u"空白"]
    print("空白数",len(empty_data))

    eff_group_id=[]
    eff_user=[]
    eff_count_sum=[]
    for item in efficient_data.groupby(['fromUsr','account']):#, 'realChatUsr'
        # print(item)
        #print(item[0])
        eff_group_id.append(item[0][0])
        eff_user.append(item[0][1])
        #print(len(item[1]))#去重复，待实现
        eff_count_sum.append(len(item[1]))

    efficient_data_sql = {
        "date":pd.Series(np.full([1,len(eff_group_id)],yesterday)[0]),
        "group": pd.Series(eff_group_id),
        "account":pd.Series(eff_user),
        "num": pd.Series(eff_count_sum)
    }
    efficient_df_sql = pd.DataFrame(efficient_data_sql,index=None)
    #efficient_df_sql.to_csv("result/efficient/%s_efficient.csv" % yesterday,index=False)

    con_group_id=[]
    con_user=[]
    con_count_sum=[]
    for item in consult_data.groupby(['fromUsr','account']):#, 'realChatUsr'
        # print(item)
        #print(item[0])
        con_group_id.append(item[0][0])
        con_user.append(item[0][1])
        #print(len(item[1]))#去重复，待实现
        con_count_sum.append(len(item[1]))
    consult_data_sql = {
        "date":pd.Series(np.full([1,len(con_group_id)],yesterday)[0]),
        "groupid": pd.Series(con_group_id),
        "account": pd.Series(con_user),
        "consult_num": pd.Series(con_count_sum)
    }
    print("咨询条数",len(con_group_id))
    consult_df_sql = pd.DataFrame(consult_data_sql,index=None)
    #consult_df_sql.to_csv("result/consult/%s_consult.csv" % yesterday,index=False)
    
    peo_group_id=[]
    peo_real_id=[]
    peo_user=[]
    #peo_count_sum=[]
    for item in consult_data.groupby(['fromUsr','realChatUsr','account']):#, 'realChatUsr'
        # print(item)
        #print(item[0])
        peo_group_id.append(item[0][0])
        #print(len(item[1]))#去重复，待实现
        peo_real_id.append(item[0][1])

        peo_user.append(item[0][2])
        #peo_count_sum.append(len(item[2]))
    people_data_sql = {
        #"date":pd.Series(np.full([1,len(peo_group_id)],yesterday)[0]),
        "groupid": pd.Series(peo_group_id),
        "people_id": pd.Series(peo_real_id),
        "account": pd.Series(peo_user)
    }
    people_df_sql = pd.DataFrame(people_data_sql,index=None)
    #consult_df_sql.to_csv("result/consult/%s_consult.csv" % yesterday,index=False)
    
    #real_group_id=[]
    real_count_sum=[]
    for item in people_df_sql.groupby(['account','groupid']):#, 'realChatUsr'
        # print(item)
        #print(item[0])
        #real_group_id.append(item[0][0])
        #print(len(item[1]))#去重复，待实现
        real_count_sum.append(len(item[1]))

    consult_data_sql['people_num']=real_count_sum
    consult_df_sql = pd.DataFrame(consult_data_sql,index=None)

    remove_group=[]
    remove_id=[]
    remove_user=[]
    for item in remove_data.groupby(['fromUsr','realChatUsr','account']):#, 'realChatUsr'
        remove_group.append(item[0][0])
        remove_id.append(item[0][1])
        remove_user.append(item[0][2])

    remove_data_sql = {
        "date":pd.Series(np.full([1,len(remove_group)],yesterday)[0]),
        "groupid": pd.Series(remove_group),
        "account": pd.Series(remove_user),
        "userid": pd.Series(remove_id)
    }
    remove_df_sql = pd.DataFrame(remove_data_sql,index=None)
    #remove_df_sql[~remove_df_sql['userid'].isin(clientsale)].to_csv("result/remove/%s_remove.csv" % yesterday,index=False)
    
    commend_group=[]
    commend_id=[]
    commend_user=[]
    commend_count=[]
    for item in commend_data.groupby(['fromUsr','realChatUsr','account']):#, 'realChatUsr'
        commend_group.append(item[0][0])
        commend_id.append(item[0][1])
        commend_user.append(item[0][2])
        commend_count.append(len(item[1]))

    commend_data_sql = {
        "date":pd.Series(np.full([1,len(commend_group)],yesterday)[0]),
        "groupid": pd.Series(commend_group),
        "account": pd.Series(commend_user),
        "userid": pd.Series(commend_id),
        "commend_num": pd.Series(commend_count)
    }
    print("推荐数",len(commend_count))
    commend_df_sql = pd.DataFrame(commend_data_sql,index=None)
    #remove_df_sql[~remove_df_sql['userid'].isin(clientsale)].to_csv("result/remove/%s_remove.csv" % yesterday,index=False)

    #入库
    #清洗realid
    #[\d-]+[,\d@]+[a-z]+[,]
    mysql_obj1=mysql_ORM("rm-bp171b759ha99x5wfso.mysql.rds.aliyuncs.com","root","HPGQEhutFBUCi8ZE8JYgWDwZVhAHXWJx",3306,"businessdata")
    mysql_obj2=mysql_ORM("127.0.0.1","root","smy123456",3306,"BusinessData")
    mysql_conn1=mysql_obj1.connect_fc()
    mysql_conn2=mysql_obj2.connect_fc()
    
    print("start mysql",yesterday)
    db_sql1='insert into chatdata_classify(date,content,msgType,owner_wxid,realChatUsr,fromUsr,toUsr,repeat_num,account,classify) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    db_sql2='insert into chatdata_remove(date,groupid,account,userid) values (%s,%s,%s,%s)'
    db_sql3='insert into chatdata_consult(date,groupid,account,consult_num,people_num) values (%s,%s,%s,%s,%s)'
    db_sql4='insert into chatdata_commend(date,groupid,account,userid,commend_num) values (%s,%s,%s,%s,%s)'
    
    data.drop(['index'],axis=1,inplace=True)
    
    mysql_obj1.mult_add(mysql_conn1,tuple((np.array(data.values)).tolist()),db_sql1)
    mysql_obj2.mult_add(mysql_conn2,tuple((np.array(data.values)).tolist()),db_sql1)
    mysql_obj1.mult_add(mysql_conn1,tuple((np.array(remove_df_sql[~remove_df_sql['userid'].isin(clientsale)].values)).tolist()),db_sql2)
    mysql_obj2.mult_add(mysql_conn2,tuple((np.array(remove_df_sql[~remove_df_sql['userid'].isin(clientsale)].values)).tolist()),db_sql2)
    mysql_obj1.mult_add(mysql_conn1,tuple((np.array(consult_df_sql.values)).tolist()),db_sql3)
    mysql_obj2.mult_add(mysql_conn2,tuple((np.array(consult_df_sql.values)).tolist()),db_sql3)
    mysql_obj1.mult_add(mysql_conn1,tuple((np.array(commend_df_sql.values)).tolist()),db_sql4)
    mysql_obj2.mult_add(mysql_conn2,tuple((np.array(commend_df_sql.values)).tolist()),db_sql4)
    
    print("end mysql",yesterday)
    

'''
SELECT content,
msgType,
owner_wxid,
realChatUsr,
fromUsr,
toUsr,count(1) as num FROM businessdata.chatdata group by owner_wxid;
select DATE_SUB(curdate(),INTERVAL 1 DAY)='2019-03-24';
select substring_index(FROM_UNIXTIME(createTime),' ',1) from businessdata.chatdata;

select createTime,content,msgType,owner_wxid,realChatUsr,fromUsr,toUsr,count(1) as num from(
SELECT substring_index(FROM_UNIXTIME(createTime),' ',1) as createTime,content,msgType,owner_wxid,realChatUsr,fromUsr,toUsr,count(1) as num from businessdata.chatdata
where msgType='TEXT' and substring_index(FROM_UNIXTIME(createTime),' ',1)='2019-03-24' #content like "%@%"
group by fromUsr,realChatUsr
order by fromUsr) A
group by A.fromUsr;

# 查找重复
select * from (
(select * from businessdata.chatdata) a
right join
(
SELECT createTime,content,msgType,owner_wxid,realChatUsr,fromUsr,toUsr,count(1) as num from businessdata.chatdata
where substring_index(FROM_UNIXTIME(createTime),' ',1)='2019-03-26' and content<>0 #content like "%@%"
group by fromUsr,realChatUsr,toUsr,content,createTime,owner_wxid
having num>1
order by fromUsr
) b
on a.createTime=b.createTime and a.fromUsr=b.fromUsr and a.content=b.content
);


# 
select * from (
(select * from businessdata.chatdata) a
right join
(
SELECT createTime,content,msgType,owner_wxid,realChatUsr,fromUsr,toUsr,count(1) as num from businessdata.chatdata
where substring_index(FROM_UNIXTIME(createTime),' ',1)='2019-03-25' #content like "%@%"
group by createTime,content,fromUsr,realChatUsr,toUsr
order by fromUsr
) b
on a.createTime=b.createTime and a.fromUsr=b.fromUsr and a.content=b.content
);


#连续三天都发一条消息的人
SELECT createTime,content,msgType,owner_wxid,realChatUsr,fromUsr,toUsr,count(1) as num from businessdata.chatdata
where content<>0
group by fromUsr,realChatUsr,toUsr,content
having num>1;

'''
