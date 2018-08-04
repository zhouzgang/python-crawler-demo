# -*- coding:utf-8 -*-
import json
import time
import psycopg2
import logging


# 简单的logging配置
# logging.basicConfig(level=logging.DEBUG,
#                     format='[%(asctime)s %(filename)s [line:%(lineno)d]] %(levelname)s %(message)s',
#                     datefmt='%a, %d %b %Y %H:%M:%S',
#                     filename='myapp.log',
#                     filemode='w')


# 构建地铁信息数据
def build_station_sql(station):
    print("------构建地铁信息数据------")
    code = station['code']
    name = station['name']
    latitude = station['y']
    longitude = station['x']
    address = ''
    if 'address' in station:
        address = station['address']
    create_date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    sql = "INSERT INTO  tbl_sub_station (\"code\", \"name\", \"longitude\", \"latitude\", \"address\", \"create_date\") " \
          "VALUES ({0}, \'{1}\', {2}, {3}, \'{4}\', \'{5}\') RETURNING id" \
        .format(code, name, longitude, latitude, address, create_date)
    print("build_station_sql: ", sql)
    return sql


# 构建地铁入口数据
def build_entrance_sql(entrances, subway_station_id):
    print("------构建地铁入口数据------")
    if len(entrances) == 0:
        return ''
    sql_list = []
    for entrance in entrances:
        short_name = entrance['short_name']
        sql = "INSERT INTO tbl_sub_entrance(\"short_name\", \"subway_station_id\") " \
              "VALUES (\'{0}\', {1}) RETURNING id;".format(short_name, subway_station_id)
        sql_list.append(sql)
    print("build_entrance_sql: ", sql_list)
    return sql_list


# 构建地铁入口建筑物数据
def build_landmark_sql(landmarks, sub_entrances_id):
    print("------构建地铁入口建筑物数据------")
    sql = "INSERT INTO tbl_sub_landmark(\"name\", \"longitude\", \"latitude\", \"content\", \"type\", \"sub_entrance_id\") VALUES "
    if len(landmarks) == 0:
        return ''
    for landmark in landmarks:
        name = landmark['name']
        if '\'' in name:
            name = name.replace('\'', "’")
        latitude = landmark['y']
        longitude = landmark['x']
        if 'businfo_line_keys' in landmark:
            content = landmark['businfo_line_keys']
            type = 1
        else:
            content = "建筑物"
            type = 2
        sql += "(\'{0}\', {1}, {2}, \'{3}\', {4}, {5}),".format(name, longitude, latitude, content, type,
                                                                sub_entrances_id)
    print("build_landmark_sql: ", sql)
    return sql


# 构建地铁线路数据
def build_busline_sql(bus_line_list, sub_station_id):
    print("------构建地铁线路数据------")
    sql = "INSERT INTO tbl_sub_busline(\"name\", \"front_name\", \"terminal_name\", \"line_id\", \"stations\", " \
          "\"start_time\", \"end_time\", \"current_start_time\", \"current_end_time\", \"sub_station_id\") VALUES "
    if len(bus_line_list) == 0:
        return ''
    for bus_line in bus_line_list:
        name = ''
        if 'name' in bus_line:
            name = bus_line['name']
        bus_lines = bus_line['lines']
        for line in bus_lines:
            front_name = ''
            if 'front_name' in line:
                front_name = line['front_name']
            terminal_name = ''
            if 'terminal_name' in line:
                terminal_name = line['terminal_name']
            line_id = 0
            if 'id' in line:
                line_id = line['id']
            start_time = ''
            if 'start_time' in line:
                start_time = line['start_time']
            end_time = ''
            if 'end_time' in line:
                end_time = line['end_time']
            current_start_time = ''
            if 'current_start_time' in line:
                current_start_time = line['current_start_time']
            current_end_time = ''
            if 'current_end_time' in line:
                current_end_time = line['current_end_time']
            stations = ''
            if 'stations' in line:
                stations = join_station(line['stations'])

            sql += "(\'{0}\', \'{1}\', \'{2}\', {3}, \'{4}\', \'{5}\',\'{6}\', \'{7}\', \'{8}\', {9})," \
                .format(name, front_name, terminal_name, line_id, stations,
                        start_time, end_time, current_start_time, current_end_time, sub_station_id)
    print("build_busline_sql: ", sql)
    return sql


def join_station(station_list):
    stations = ''
    for item in station_list:
        if 'changes' in item:
            stations += item['name'] + '(' + ','.join(item['changes']) + '),'
        else:
            stations += item['name'] + ','
    stations = stations[:-1]
    return stations


def save_entrances(conn, station, station_id):
    if ('subway' not in station['data']) \
            or ('entrances_landmarks' not in station['data']['subway']):
        return
    entrances_sql_list = build_entrance_sql(station['data']['subway']['entrances_landmarks'], station_id[0])
    for index, item in enumerate(entrances_sql_list):
        entrance_id = execute_sql(conn, item)
        bus = []
        lab = []
        if 'landmark_bus_stations' in station['data']['subway']['entrances_landmarks'][index]:
            bus = station['data']['subway']['entrances_landmarks'][index]['landmark_bus_stations']
        if 'landmarks' in station['data']['subway']['entrances_landmarks'][index]:
            lab = station['data']['subway']['entrances_landmarks'][index]['landmarks']
        landmarks = bus + lab
        landmark_sql_list = build_landmark_sql(landmarks, entrance_id[0])
        landmark_sql_list = landmark_sql_list[:-1]
        if landmark_sql_list != '':
            execute_sql_no_result(conn, landmark_sql_list)


def execute_sql(conn, sql):
    cursor = conn.cursor()
    cursor.execute(sql)
    id = cursor.fetchone()
    conn.commit()
    cursor.close()
    return id


def execute_sql_no_result(conn, sql):
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    cursor.close()


def db_conn_config():
    conn = psycopg2.connect(database='joy',
                            user='postgres',
                            password='postgres',
                            host='10.0.0.112',
                            port='5432')
    return conn


# 找出数据，
# 拼装成 sql 语句
# 执行
def station_maim():
    # ./test_python.json
    with open("./test_python.json", "r", encoding="utf-8") as load_f:
        stations = json.load(load_f)
        conn = db_conn_config()
        for idex, station in enumerate(stations):
            print('-----start: ', idex)
            station_sql = build_station_sql(station['data']['base'])
            station_id = execute_sql(conn, station_sql)

            save_entrances(conn, station, station_id)

            busline_sql_list = build_busline_sql(station['data']['busline_list'], station_id[0])
            busline_sql_list = busline_sql_list[:-1]
            if busline_sql_list != '':
                execute_sql_no_result(conn, busline_sql_list)
            print('-----end: ', idex)


# 确定需要启动数据导入
station_maim()
