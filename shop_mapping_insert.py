"""
1、读excel获取乐行中爬取大众点评的某个商场下的店铺数据（包含乐行商铺id，地址和商铺名称）
2、拿着第一步的数据信息去es库上匹配，找到shopCode
3、将seller_id 和 shopCode关联起来存进数据库
"""

import psycopg2
import xlrd
import urllib.request
import json


def open_excel(file='D:\\store.xlsx'):
    try:
        data = xlrd.open_workbook(file)
        return data
    except Exception as e:
        print(str(e))


def excel_table_byname(file='D:\\store.xlsx', colnameindex=0, by_name='Sheet1'):  # 修改自己路径
    data = open_excel(file)
    table = data.sheet_by_name(by_name)  # 获得表格
    nrows = table.nrows  # 拿到总共行数
    colnames = table.row_values(colnameindex)  # 某一行数据 ['姓名', '用户名', '联系方式', '密码']
    list = []
    for rownum in range(1, nrows):  # 也就是从Excel第二行开始，第一行表头不算
        row = table.row_values(rownum)
        if row:
            app = {}
            for i in range(len(colnames)):
                app[colnames[i]] = row[i]  # 表头与数据对应
            list.append(app)
    return list


def run_excel(file='D:\\store.xlsx', colnameindex=0,
              by_name='Sheet4'):  # 修改自己路径
    data = open_excel(file)
    table = data.sheet_by_name(by_name)  # 获得表格
    nrows = table.nrows  # 拿到总共行数
    list = []
    for rownum in range(0, nrows):
        row = table.row_values(rownum)
        if row:
            map = {}
            map['seller_id'] = int(row[0])
            map['shop_code'] = int(row[3])  # UPDATE_01
            list.append(map)

    print("excel's size is: " + str(len(list)))
    return list


def getData(conn, sql):
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    result_list = []
    for r in rows:
        result_list.append(r)
    # conn.commit()
    cursor.close()
    # conn.close()
    return result_list


def executeSql(conn, sql):
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    # conn.close()


def main():
    # excel_name = "D:\\workspace\\project\\pycharm\\joy-data-center\\seller\\北京\\skp_false_new_7.xlsx"
    # excel_name = "C:\\Users\\Administrator\\Downloads\\sz\\1358.csv"
    # tables = run_excel(excel_name,0,'Sheet1')
    conn = psycopg2.connect(database='joy',
                            user='postgres',
                            password='postgres',
                            host='192.168.0.204',
                            port='5432')
    sceneIds = [
        "25911312_2946"
        , "26391137_3640"
    ]
    for sceneId in sceneIds:
        # sceneId = '3649'
        remark = '北京-' + sceneId + '-店铺数据打通'
        file = open("D:\\workspace\\project\\pycharm\\joy-data-center\\seller\\" + sceneId, 'rb')

        i = 0
        total = 0
        for line in file:
            total = total + 1
            l = line.decode('unicode-escape')
            # print(l)
            split = l.split('\t')
            sellerId = split[0]
            shopCode = split[3]
            # print(str(sellerId) + "=======" + str(shopCode))
            sql = "insert into tbl_mapping_joy_seller values({0},{1},'{2}');".format(shopCode, sellerId, remark)
            print(sql)
            try:
                executeSql(conn, sql)
                i = i + 1
            except Exception as e:
                print('错误：执行sql语句异常！sql语句为： %s ' % (sql))
                print('出错信息：' + str(e))
                conn.rollback()

        print("总数：" + str(total) + "     总录入数：" + str(i))


if __name__ == "__main__":
    main()
