# -*- coding: utf-8 -*-
"""
    author：Wo_O3
    date: 2019-11-7 16:53:02
    vesion:
        2019-11-15 15:05:21 优化输出界面,更换数据库引擎,简化逻辑
        2019-11-19 11:03:47 修改get_table函数,增加筛选
        2019-11-25 16:08:52 修复bug,过滤空表,优化正则规则
        2020-4-17 11:38:33 优化注释结构
    function: 早期同步脚本升级版,使用pandas作为同步工具库,增加配置文件,分离代码
        与作业,实现数据库A到数据B的同步程序
"""

import os
import re
import copy
import time
from datetime import datetime
import pandas as pd
import pymysql
from sqlalchemy import create_engine

__version__ = '1.0.1'

def user_interface(args):
    '''
    控制台输出
    '''
    date = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
    print('='*87)
    print('=', ' '*29, 'Synchronous Information', ' '*29, '=')
    print('='*87)
    print(f"=  source database: {args['source']['database']:<19} "
          f"\ttarget database: {args['target']['database']:<19}  =")
    print(f"=  source host: {args['source']['host']:<23} "
          f"\ttarget host: {args['target']['host']:<23}  =")
    print(f"=  source database_size: {args['source']['size']:<14} "
          f"\tsynchronous table size: {args['target']['size']:<12}  =")
    print(f"=  table number: {args['source']['number']:<22} "
          f"\tsynchronous table number: {args['target']['number']:<10}  =")
    print(f"=  filter table: {args['source']['filter']:<67}", " =")
    print(f"=  choose table: {args['source']['choose']:<67}", " =")
    print('=', ' '*83, '=')
    print("=", ' '*48, f'current time: {date}  =')
    print("=", ' '*46, 'estimated time: ', f'{args["estimated_time"]:<10} second   =')
    print('='*87)
    print('=', ' '*64, 'version: ', f'{__version__:^8}', '=')
    print('='*87)

def get_database_info(cursor, database):
    '''
    获取数据库基本信息
    '''
    database_size_sql = f"""
        select table_schema, concat(truncate(sum(data_length),2),' bytes') as database_size
        from information_schema.tables
        where table_schema = '{database}'
        group by table_schema
        """
    cursor.execute(database_size_sql)
    datasize = cursor.fetchall()[0]['database_size']
    cursor.execute(
        f'select TABLE_NAME from information_schema.tables where table_schema = "{database}"'
        )
    result = cursor.fetchall()
    result
    database_info = {}
    database_info['name'] = database
    database_info['size'] = datasize
    database_info['tables'] = [list(r.values())[0] for r in result]     
    database_info['number'] = len(result)
    return database_info

def get_table_info(cursor, database, table):
    '''
    获取表格大小以及表格列属性
    '''
    column_sql = f"""
        select COLUMN_NAME ,COLUMN_TYPE ,COLUMN_COMMENT 
        from information_schema.`COLUMNS` 
        where TABLE_NAME = '{table}' and table_schema = '{database}'
        """
    cursor.execute(column_sql)
    columns_info = cursor.fetchall()
    table_size_sql = f"""
        select TABLE_NAME, concat(truncate(data_length,2),' bytes') as table_size, TABLE_COMMENT
        from information_schema.tables
        where TABLE_NAME = '{table}' and table_schema = "{database}"
        """
    cursor.execute(table_size_sql)
    # table_size = cursor.fetchall()[0]['table_size']
    # TABLE_COMMENT =  cursor.fetchall()[0]['TABLE_COMMENT']
    table_info = cursor.fetchall()
    # return columns_info, table_size, TABLE_COMMENT
    return columns_info, table_info

def get_table(cursor, database, table, **columns):
    '''
    获取表格数据,columns参数接受字符串数据完成where条件筛选,请多加一层引号,
    '''
    if columns == {}:
        cursor.execute(f'select * from {database}.{table}')
    else:
        santence = " where "
        for i in columns:
            column = f"{i} = {columns[i]}"
            santence = santence + column + ' and '
        santence = santence[:-5]
        cursor.execute(f'select * from {database}.{table}' + santence)
    table_ = cursor.fetchall()
    table_ = pd.DataFrame(table_)
    return table_

def is_exists_table(cursor, database, table):
    '''
    判断目标表是否存在
    '''
    tag = cursor.execute(f'show tables from "{database}" like "{table}"')
    return tag

def is_exists_database(cursor, database):
    '''
    判断目标数据库是否存在
    '''
    tag = cursor.execute(f"show databases like '{database}'")
    return tag

def create_target_database(cursor, database):
    '''
    创建目标数据库
    '''
    cursor.execute(f'CREATE DATABASE IF NOT EXISTS {database}')

def create_target_table(cursor, database, table_info, columns_info):
    '''
    创建目标表
    '''
    columns = ['`'+c['COLUMN_NAME'] + '` ' + c['COLUMN_TYPE'] + ' comment "' + c['COLUMN_COMMENT'] + '"'  for c in columns_info]
    columns = ",".join(columns)
    print(f"CREATE TABLE IF NOT EXISTS {database}.{table_info[0]['TABLE_NAME']} ({columns})comment='{table_info[0]['TABLE_COMMENT']}'")
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {database}.{table_info[0]['TABLE_NAME']} ({columns})comment='{table_info[0]['TABLE_COMMENT']}'")

def drop_table(cursor, database, table):
    '''
    删除目标表
    '''
    cursor.execute(f'DROP TABLE IF EXISTS {database}.{table}')

def drop_database(cursor, database):
    '''
    删除目标数据库
    '''
    cursor.execute(f'DROP DATABASE IF EXISTS {database}')

def add_table_comment(cursor, database, table_info):
    cursor.execute(f'alter table {database}.{table_info["TABLE_NAME"]} comment "{table_info["TABLE_COMMENT"]}"')

def add_columns_comment(cursor, database, table, columns_info):
    column_sql = f"""
        select COLUMN_NAME ,COLUMN_TYPE ,COLUMN_COMMENT 
        from information_schema.`COLUMNS` 
        where TABLE_NAME = '{table}' and table_schema = '{database}'
        """
    cursor.execute(column_sql)
    columns_info_target = cursor.fetchall()
    df1 = pd.DataFrame(columns_info_target)
    df2 = pd.DataFrame(columns_info)
    df = pd.merge(df1, df2, on='COLUMN_NAME',how='left')
    for i in range(len(df)):
        print(f'alter table {database}.{table} modify column `{df["COLUMN_NAME"].loc[i]}` {df["COLUMN_TYPE_x"].loc[i]} comment "{df["COLUMN_COMMENT_y"].loc[i]}"')
        cursor.execute(f'alter table {database}.{table} modify column `{df["COLUMN_NAME"].loc[i]}` {df["COLUMN_TYPE_x"].loc[i]} comment "{df["COLUMN_COMMENT_y"].loc[i]}"')

class DatabaseConnect:
    '''
    实例化数据库连接
    '''
    def __init__(self, config):
        '''
        配置信息,连接数据库
        '''
        self.config = config
        self.source = config['source']
        self.target = config['target']
        try:
            self.source_client = pymysql.connect(source['host'], source['user'], source['password'])
            self.source_cursor = self.source_client.cursor(cursor=pymysql.cursors.DictCursor)
        except Exception as e:
            print(f'{"ERROR":^8}：源数据库错误信息：', e)
            self.close()
        else:
            print(f'{"INFO":^8}：信息校验通过,源数据库连接成功！')
        try:
            self.target_client = pymysql.connect(target['host'], target['user'], target['password'])
            self.target_cursor = self.target_client.cursor(cursor=pymysql.cursors.DictCursor)
        except Exception as e:
            print(f'{"ERROR":^8}：目标数据库错误信息：', e)
            self.close()
        else:
            print(f'{"INFO":^8}：信息校验通过,目标数据库连接成功！')

    def close(self):
        '''
        关闭数据库连接
        '''
        try:
            self.source_client.close()
        except:
            pass
        try:
            self.target_client.close()
        except:
            pass

class Synchronous(DatabaseConnect):
    '''
    执行同步
    '''
    def __init__(self, config, match='greedy'):
        '''
        初始化,验证同步数据库信息以及目标数据是否存在
        '''
        super().__init__(config)
        source = self.source
        target = self.target
        source_cursor = self.source_cursor
        target_cursor = self.target_cursor
        # 源数据库检查
        tag = is_exists_database(source_cursor, source['database'])
        if not tag:
            print(f'{"WARNING":^8}：源数据库-{source["database"]:^30}-不存在,请确认同步信息,再次同步！')
            return
        # 目标数据库检查
        tag = is_exists_database(target_cursor, target['database'])
        if not tag:
            create_target_database(target_cursor, target['database'])
            print(f'{"INFO":^8}：目标数据库-{target["database"]:^30}-不存在,已创建同名数据库')
        # 工作队列清洗
        table_list = get_database_info(source_cursor, source['database'])['tables']
        filter_list = copy.deepcopy(table_list)
        choose_list = []
        if match == 'greedy':
            table_list_copy = self._filter_choose(source['filter'], filter_list, table_list, 'filter')
            table_list = self._filter_choose(source['choose'], choose_list, table_list_copy, 'choose')
        elif match == 'non-greedy':
            filter_list = self._filter_choose(source['choose'], choose_list, table_list, 'choose')
            filter_list_copy = copy.deepcopy(filter_list)
            table_list = self._filter_choose(source['filter'], filter_list_copy, filter_list, 'filter')
        self.table_set = set(table_list)

    def _filter_choose(self, pattern_string, work_list, refer_list, para):
        '''
        根据正则字符串pattern_string,过滤work_list
        '''
        if not pd.isna(pattern_string):
            pattern = pattern_string.split('\\')
            for p in pattern:
                for t in refer_list:
                    if re.match(p, t):
                        if para == 'filter':
                            work_list.remove(t)
                        elif para == 'choose':
                            work_list.append(t)
        else:
            return refer_list
        return work_list

    def main(self, **columns):
        ''''
        同步程序入口
        '''
        # 同步模式确认
        mode = self.config['common']['mode']
#        if mode == 'replace':
#            tag = input(f'{"WARNING":^8}：当前同步模式为【覆盖模式】,该操作将会清除'
#                        '原始已有的原始数据,不可恢复!'
#                        '\n          确认同步请输入【Y】,输入任意键退出:')
#            if tag != 'Y':
#                self.close()
#                return
#        elif mode == 'append':
#            tag = input(f'{"WARNING":^8}：当前同步模式为【插入模式】,请检查表结构!'
#                        '\n          确认同步请输入【Y】,输入任意键退出:')
#            if tag != 'Y':
#                self.close()
#                return
#        else:
#            self.close()
#            raise ValueError('mode参数只接受"replace"和"append",默认为"replace"')
        # 基本属性
        source = self.source
        target = self.target
        source_cursor = self.source_cursor
        target_cursor = self.target_cursor
        target_engine = create_engine(
            f"mysql+pymysql://{target['user']}:{target['password']}"
            f"@{target['host']}:3306/{target['database']}"
            )
        # 控制台输出同步信息
        work_size = 0
        for table in self.table_set:
            table_size = get_table_info(source_cursor, source['database'], table)[1][0]['table_size']
            work_size = work_size + float(table_size[:-6])
        database_info = get_database_info(source_cursor, source['database'])
        arg = self.config
        arg['source']['size'] = str(round(float(database_info['size'][:-6])/1024/1024,2)) + ' mb'
        arg['source']['number'] = database_info['number']
        arg['target']['size'] = str(round(work_size/1024/1024,2)) + ' mb'
        arg['target']['number'] = len(self.table_set)
        arg['estimated_time'] = int(work_size/1024/1024/80*60)
        user_interface(arg)
        # 开始同步,输出同步信息
        print(f'\n{"-_"*19}[ S T A R T ]{"-_"*18}\n')
        count = 1
        start = time.time()
        for table in self.table_set:
            s = time.time()
            table_columns_info = get_table_info(source_cursor, source['database'], table)
            table_size = table_columns_info[1][0]['table_size']
            table_spend = int(float(table_size[:-6])/1024/1024/80*60)
            print(f'--->当前同步表：{table:<20}\t数据表大小：{table_size}\n--->预计耗时：{table_spend:^10} second')
            data = get_table(source_cursor, source['database'], table, **columns)
            if len(data) and mode == 'replace':
                drop_table(target_cursor, target['database'], table)
                create_target_table(target_cursor, target['database'], table_columns_info[1], table_columns_info[0])
                data.to_sql(table, target_engine, if_exists='append', chunksize=2000, index=0)
                e = time.time()
                print(f'--->目标表：{table:<20} 已完成同步\n--->实际耗时：{int(e-s):^10} second\n{"-"*86}>')
            elif len(data) and mode == 'append':
                data.to_sql(table, target_engine, if_exists='append', chunksize=2000, index=0)
                e = time.time()
            else:
                print(f'--->WARNING:目标表{table:<20}为空,已跳过同步\n{"-"*86}>')
            count += 1
        end = time.time()
        spend = int(end - start)
        print(f'--->当前任务已完成,预计耗时{arg["estimated_time"]:^10} second,实际耗时{spend:^10} second')
        print(f'{"-_"*20}[ E N D ]{"-_"*19}\n')
        self.close()

if __name__ == '__main__':
    # 读取配置文件
    PATH = os.path.dirname(os.path.realpath(__file__))
    CONFIG = pd.read_excel(PATH + '/Config.xlsx', sheet_name=None)
    for mission in list(CONFIG.keys()):
        source = {
            'host':CONFIG.get(mission).source[0],
            'user':CONFIG.get(mission).source[1],
            'password':CONFIG.get(mission).source[2],
            'database':CONFIG.get(mission).source[3],
            'filter':CONFIG.get(mission).source[6],
            'choose':CONFIG.get(mission).source[7]
            }
        target = {
            'host':CONFIG.get(mission).target[0],
            'user':CONFIG.get(mission).target[1],
            'password':CONFIG.get(mission).target[2],
            'database':CONFIG.get(mission).target[3]
            }
        common = {
            'mode':CONFIG.get(mission).source[5],
            'switch':CONFIG.get(mission).source[8]
            }
        mission_config = {'source':source, 'target':target, 'common':common}
        if mission_config['common']['switch'] == 'on':
            print(f'{"NOTICE":^8}：当前启动执行任务【{mission}】')
#            load_date = datetime.datetime.strftime(
#                datetime.date(
#                    datetime.datetime.now().year- (datetime.datetime.now().month==1),
#                    datetime.datetime.now().month - 1 or 12, 1
#                    ),
#                '%Y%m01'
#                )
#            Synchronous(mission_config).main(load_date = f"'{load_date}'")
            Synchronous(mission_config).main()
        else:
            print(f'{"NOTICE":^8}：任务【{mission}】状态关闭,跳过当前任务')
