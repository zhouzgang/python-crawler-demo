# -*- coding:utf-8 -*-
import json
import time
import psycopg2


# print("hello py")
# print('The quick brown fox', 'jumps over', 'the lazy dog')

# age = int(input('age:'))

# if age >= 18:
#     print("hee")
# elif age >= 2:
#     print("guol")
# else:
#     print("yyy")


# names = ['zhangsan', 'lisi', 'wangwu']
# for item in names:
#     print(item)

# for item in ['zhangsan', 'lisi', 'wangwu']:
#     print(item)

# def bianLi():
#     d = {'ma':98, 'ha':12, 'ya':68}
#     print(len(d))
#     for item in d:
#         print(item, d[item])

# bianLi()

# print("--------------")
# def test(name):
#     if(name == "tt"):
#         print("haode")
#     elif(name == 'rr'):
#         return "yy", "tyty"        
#     else: 
#         bianLi()
#         print('-------')
#         return 123

# # 为什么这里会多一个 None？
# print(test("naadfa"))

# print(test('rr'))

# 必选参数在前，默认参数在后，否则Python的解释器会报错（思考一下为什么默认参数不能放在必选参数前面）；
# def power(x, n=3):
#     print(x, n)

# power(1)
# power(1, 8)

# def calc(*num):
#     for n in num:
#         print(n)

# calc(1,2,23,4)

# l = [1,12,23,1,141,412,12,412,41,41,4,1241,24,12,412,41,4,1]
# print(l[:10])




# json_file = "D:\\workspace\\python\\test_python.json"
# fb = open(json_file)
# print(fb)
# print('-----------------')
# dicts = json.loads(fb)
# print(dicts)


# load:把文件打开，并把字符串变换为数据类型
# print(open("./test_python.json", "r", encoding="utf-8"))
# def open():
with open("./test_python.json", "r", encoding="utf-8") as load_f:
    stations = json.load(load_f)
    for station in stations:
        print(station)

    # 地铁站出口
    # print(dict_3['data']['subway'])
    # 地铁站进过路线
    # print(dict_3['data']['busline_list'])

# open()

# 构建地铁信息数据
def subt_station(station):
    code = station['base']['code']
    name = station['base']['name']
    latitude = station['base']['y']
    longitude = station['base']['x']
    address = station['base']['address']
    create_date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


# 构建地铁入口数据
def subt_entrance(station):
    short_name = station['subway']['entrances_landmarks']['short_name']
    short_name = station['']


def conn_db():
    conn = psycopg2.connect(database='joy',
                            user='postgres',
                            password='postgres',
                            host='10.0.0.112',
                            port='5432')
    cur = conn.cursor()
    cur.execute("SELECT * FROM tbl_sub_entrance LIMIT 10")
    rows = cur.fetchall()
    print(rows)
    conn.commit()
    cur.close()

# conn_db()



# assert isinstance(dict_3, dict)
# test_dict = {'bigberg': [7600, {1: [['iPhone', 6300], ['Bike', 800], ['shirt', 300]]}]}
# print(test_dict['bigberg'])
# print(type(test_dict))


# with open("test_python.json", 'r') as f:
#     temp = json.loads(f.read())
#     print(temp)
#     print(temp['rule'])
#     print(temp['rule']['namespace'])