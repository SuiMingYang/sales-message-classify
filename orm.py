#coding:utf8

import pymysql.cursors

class mysql_ORM():
    def __init__(self,host,user,passwd,port,db):
        self.host=host
        self.user=user
        self.passwd=passwd
        self.port=port
        self.db=db

    def connect_fc(self):
        connect = pymysql.Connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.passwd,
            db=self.db,
            charset='utf8mb4'
            #插入中文问题
        )   
        return connect
        # 获取游标
    
    def mult_add(self,connect,mult_data,dbsql):
        # 批量插入
        cursor = connect.cursor()
        sql = dbsql
        #print(mult_data)
        try:
            cursor.executemany(sql, mult_data)
        except Exception as e:
            connect.rollback()  # 事务回滚
            print('事务处理失败', e)
            cursor.close()
            #connect.close()
        else:
            connect.commit()  # 事务提交
            print('事务处理成功', cursor.rowcount)
            print('成功插入', cursor.rowcount, '条数据')
            cursor.close()
            #connect.close()
    
    def add(self,connect,data):
        cursor = connect.cursor()
        sql = 'insert into socialgroup(date,user,group_count,people_count,channelid) values (%s,%s,%s,%s,%s)'
        cursor.executemany(sql,data)
        connect.commit()
        print('成功插入', cursor.rowcount, '条数据')
        cursor.close()
        connect.close()


if __name__ == "__main__":
    obj=mysql_ORM("127.0.0.1","root","smy123456",3306,"BusinessData")
    #obj=mysql_ORM("127.0.0.1","root","smy123456",3306,"DecisionAnalysis")
    conn=obj.connect_fc()
    
    obj.add(conn,('2019-02-28', 'gaoliping', 1, 57, 'V3q7t8L'))
    
    #,('TR190101085714251C43687499F1F444', '98169', '2018款条纹衬衫女长袖韩版宽松大码棉麻上衣中长款衬衣', '2019-01-01 08:43:39', '无', '69', '69', '13861728466', '社群测试组-A组', '支付超时', '老用户')
# 插入数据
'''

'''
'''
# 修改数据
sql = "UPDATE trade SET saving = %.2f WHERE account = '%s' "
data = (8888, '13512345678')
cursor.execute(sql % data)
connect.commit()
print('成功修改', cursor.rowcount, '条数据')

# 查询数据
sql = "SELECT name,saving FROM trade WHERE account = '%s' "
data = ('13512345678',)
cursor.execute(sql % data)
for row in cursor.fetchall():
    print("Name:%s\tSaving:%.2f" % row)
print('共查找出', cursor.rowcount, '条数据')

# 删除数据
sql = "DELETE FROM trade WHERE account = '%s' LIMIT %d"
data = ('13512345678', 1)
cursor.execute(sql % data)
connect.commit()
print('成功删除', cursor.rowcount, '条数据')

# 事务处理
sql_1 = "UPDATE trade SET saving = saving + 1000 WHERE account = '18012345678' "
sql_2 = "UPDATE trade SET expend = expend + 1000 WHERE account = '18012345678' "
sql_3 = "UPDATE trade SET income = income + 2000 WHERE account = '18012345678' "

try:
    cursor.execute(sql_1)  # 储蓄增加1000
    cursor.execute(sql_2)  # 支出增加1000
    cursor.execute(sql_3)  # 收入增加2000
except Exception as e:
    connect.rollback()  # 事务回滚
    print('事务处理失败', e)
else:
    connect.commit()  # 事务提交
    print('事务处理成功', cursor.rowcount)
'''
# 关闭连接
