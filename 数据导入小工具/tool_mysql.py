'''
mysql 导出入小工具
'''

import pymysql

class Mysql:
    def __init__(self, host, password, user, database, port, charset, file_name, table):
        self.host=host
        self.password=password
        self.user=user
        self.database=database
        self.port=port
        self.charset=charset
        self.file_name=file_name
        self.table=table


    #导入数据库
    def mysql_input(self):
        #链接数据库
        db=pymysql.connect(host=self.host,
                           password=self.password,
                           user=self.user,
                           database=self.database,
                           port=self.port,
                           charset=self.charset)
        #创建游标
        cur=db.cursor()
        #导入数据的sql语句
        sql="load data infile '%s' " \
            "into table %s " \
            "fields terminated by ','" \
            "lines terminated by '\n'" \
            "IGNORE 1 LINES;"%(self.file_name,self.table)

        try:
            #执行sql命令
            cur.execute(sql)
            db.commit()
        except Exception as e:
            print(e)
            #出错执行回滚
            db.rollback()
        # 关闭游标
        cur.close()

        # 关闭数据库
        db.close()

    #导出csv文件
    def mysql_output(self):
        # 链接数据库
        db = pymysql.connect(host=self.host,
                           password=self.password,
                           user=self.user,
                           database=self.database,
                           port=self.port,
                           charset=self.charset)

        # 创建游标
        cur = db.cursor()
        # 导入数据的sql语句
        sql = "select * from %s " \
              "into outfile '%s' " \
              "fields terminated by ',' " \
              "lines terminated by '\n';"%(self.table,self.file_name)

        try:
            # 执行sql命令
            cur.execute(sql)
            db.commit()
        except Exception as e:
            print(e)
            # 出错执行回滚
            db.rollback()

if __name__ == '__main__':
    sql=Mysql(host='localhost',port = 3306,
                     user='root',
                     password = 'a123456',
                     database = 'paipaidai',
                     charset='utf8',
                   file_name='C:/ProgramData/MySQL/MySQL Server 8.0/Data/LCIS.csv',
                   table='ls'
              )
    # sql.mysql_output()
    sql.mysql_input()


