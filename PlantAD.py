import pymysql
from pprint import pprint
import pandas
class AdSaveAlisql(object):
    #运行数据库和建立游标对象
    def __init__(self,table):
        self.connect = pymysql.connect(host='rm-2zes295i4313vp3n4do.mysql.rds.aliyuncs.com',
                             user='squareface',
                             password='yaojiepeng',
                             db='mysql0918',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
        # 返回一个cursor对象,也就是游标对象
        self.cursor = self.connect.cursor(cursor=pymysql.cursors.DictCursor)
        self.table = table
    #关闭数据库和游标对象
    def __del__(self):
        self.connect.close()
        self.cursor.close()
    def download(self,path):
        # path 是放文件得路径
        #将数据转化成DataFrame数据格式，保存文件名字需要重新更改下。
        data = pandas.DataFrame(self.read())
        #把id设置成行索引
        data_1 = data.set_index("id",drop=True)
        #写写入数据数据
        # filename = "/Users/squareface/PycharmProjects/mysql/mysql_{}.csv".format(self.table)
        filename= path+'/mysql_{}.csv'.format(self.table)
        pandas.DataFrame.to_csv(data_1,filename,encoding="utf_8_sig")
        print("写入成功")
    def read(self):
        #读取数据库的所有数据
        data = self.cursor.execute("""select * from %s"""%(self.table))
        field_2 = self.cursor.fetchall()
        # pprint(field_2)
        return field_2

    def create(self):
        # 根据table名字创建table
        sql = """CREATE TABLE `%s` (
                          `id` int(11) NOT NULL AUTO_INCREMENT,
                          `create_time` datetime DEFAULT NULL,
                          `ad1` float(10,5) DEFAULT NULL,
                          `ad2` float(10,5) DEFAULT NULL,
                          `ad3` float(10,5) DEFAULT NULL,
                          `ad4` float(10,5) DEFAULT NULL,
                          `ad5` float(10,5) DEFAULT NULL,
                          `ad6` float(10,5) DEFAULT NULL,
                          `ad7` float(10,5) DEFAULT NULL,
                          `ad8` float(10,5) DEFAULT NULL,
                          PRIMARY KEY (`id`) USING BTREE,
                          KEY `ht_create_time_index` (`create_time`) USING BTREE
                        ) ENGINE=InnoDB AUTO_INCREMENT=1246 DEFAULT CHARSET=utf8 COMMENT='ad表';
        """ % (self.table)
        # 简单判断下，存在表需要删除
        # self.delete()
        # print('存在表，已删除{}'.format(self.table))
        try:
            self.cursor.execute(sql)
        except:
            print("不用创建啦，存在该{}表！！！,将往该表写入ad值，如需创建请更换表名，操作将继续进行...............".format(self.table))
        else:
            print('创建{}成功'.format(self.table))

    def delete(self):
        # 根据table名字删除
        sql = """DROP TABLE IF EXISTS %s"""%(self.table)
        self.cursor.execute(sql)
        print('删除{}成功'.format(self.table))

    def write(self,ad1,ad2,ad3,ad4,ad5,ad6,ad7,ad8):
        self.cursor.execute("insert into %s(create_time,ad1,ad2,ad3,ad4,ad5,ad6,ad7,ad8) value(now(),'%s','%s','%s','%s','%s','%s','%s','%s')" %(self.table,ad1,ad2,ad3,ad4,ad5,ad6,ad7,ad8))
        self.connect.commit()
        print("ad成功写入{}表".format(self.table))

def main():
    adsave = AdSaveAlisql("adtest_calss")
    adsave.create()


if __name__ == '__main__':
    main()

