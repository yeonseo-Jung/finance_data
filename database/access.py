# necessary
import os
import sys
import time
from datetime import datetime
import pandas as pd

# db connection 
import pymysql
import sqlalchemy
    
class AccessDataBase:
    
    def __init__(self, user_name, password, db_name):
        self.today = datetime.today().strftime('%y%m%d')
        
        # user info & db
        self.user_name = user_name
        self.password = password
        self.db_name = db_name
        
        if self.db_name == 'beauty_kr':
            self.host_url = "db.ds.mycelebs.com"
        elif self.db_name == 'dart' or self.db_name == 'dart_bak':
            self.host_url = "127.0.0.1"
            # self.host_url = "localhost"
        elif self.db_name == 'ta_system':
            self.host_url = "3.39.168.186"   # Elastic IP addresses [public IP]
        else:
            raise NameError('Db name error')
        
    def _connect(self):
        ''' db connect '''
            
        port_num = 3306
        conn = pymysql.connect(host=self.host_url, user=self.user_name, passwd=self.password, port=port_num, db=self.db_name, charset='utf8')
        curs = conn.cursor(pymysql.cursors.DictCursor)
        
        return conn, curs

    def get_tbl(self, table_name, columns='all'):
        ''' db에서 원하는 테이블, 컬럼 pd.DataFrame에 할당 '''
        
        if table_name in self.get_tbl_list():
            st = time.time()
            conn, curs = self._connect()
            
            if columns == 'all':
                query = f'SELECT * FROM {table_name};'
            else:
                # SELECT columns
                query = 'SELECT '
                i = 0
                for col in columns:
                    if i == 0:
                        query += f"`{col}`"
                    else:
                        query += ', ' + f"`{col}`"
                    i += 1

                # FROM table_name
                query += f' FROM {table_name};'
            curs.execute(query)
            tbl = curs.fetchall()
            df = pd.DataFrame(tbl)
            curs.close()
            conn.close()
            
            ed = time.time()
            print(f'\n\n`{table_name}` Import Time: {round(ed-st, 1)}sec')
        else:
            df = None
            print(f'\n\n{table_name} does not exist in db')
        
        return df
        
    def insert(self, table: str, fields: tuple, values: tuple) -> None:
        _fields = ''
        for field in fields:
            if _fields == '':
                _fields +=  field
            else:
                _fields += ', ' + field
        _fields_ = '(' + _fields + ')'

        conn, curs = self._connect()

        query = f"INSERT INTO `{table}`{_fields_} VALUES{str(values)};"
        try:
            curs.execute(query)
            
        except Exception as e:
            raise e
        
        finally:
            conn.commit()
            curs.close()
            conn.close()
            
    def insert_many(self, table_name: str, fields: tuple, data: list):
        _fields = ''
        for field in fields:
            if _fields == '':
                _fields +=  field
            else:
                _fields += ', ' + field
        _fields_ = '(' + _fields + ')'
        
        values = ['%s'] * len(fields)

        _values = ''
        for value in values:
            if _values == '':
                _values +=  value
            else:
                _values += ', ' + value
        _values_ = '(' + _values + ')'

        query = f'''\
        INSERT INTO `{table_name}`{_fields_}\
        VALUES {_values_};''' 

        inserted_data = [
            [None if pd.isnull(value) else value for value in sublist]\
            for sublist in data]
        
        conn, curs = self._connect()
        try:
            curs.executemany(query, inserted_data)
            
        except Exception as e:
            raise e
        
        finally:
            conn.commit()
            curs.close()
            conn.close()

    def get_tbl_list(self):
        ''' db에 존재하는 모든 테이블 이름 가져오기 '''

        conn, curs = self._connect()

        # get table name list
        query = "SHOW TABLES;"
        curs.execute(query)
        tables = curs.fetchall()

        table_list = []
        for table in tables:
            tbl = list(table.values())[0]
            table_list.append(tbl)
        curs.close()
        conn.close()
        
        return table_list

    def get_tbl_columns(self, table_name):
        ''' 선택한 테이블 컬럼 가져오기 '''
        
        conn, curs = self._connect()

        # get table columns 
        query = f"SHOW FULL COLUMNS FROM {table_name};"
        curs.execute(query)
        columns = curs.fetchall()

        column_list = []
        for column in columns:
            field = column['Field']
            column_list.append(field)
        curs.close()
        conn.close()
        
        return column_list

    def integ_tbl(self, table_name_list, columns):
        ''' 
        db에서 컬럼이 같은 여러개 테이블 가져오기
        db에서 테이블 가져온 후 데이터 프레임 통합 (concat)
        '''

        df = pd.DataFrame()
        for tbl in table_name_list:
            df_ = self.get_tbl(tbl, columns)
            df_.loc[:, 'table_name'] = tbl
            df = pd.concat([df, df_])
        df = df.reset_index(drop=True)
        return df
    
    def engine_upload(self, upload_df, table_name, if_exists_option):
        ''' Create Table '''
        
        port_num = 3306
        engine = sqlalchemy.create_engine(f'mysql+pymysql://{self.user_name}:{self.password}@{self.host_url}:{port_num}/{self.db_name}?charset=utf8')
        
        # Create table or Replace table 
        upload_df.to_sql(table_name, engine, if_exists=if_exists_option, index=False)

        engine.dispose()
        print(f'\n\nTable upload successful: `{table_name}`')
        
    def _drop(self, table_name):
        ''' Drop Table '''
        
        query = f'drop table `{table_name}`;'
        conn, curs = self._connect()
        curs.execute(query)
        conn.commit()
        curs.close()
        conn.close()
        
        print(f'\n\nTable drop succesful: `{table_name}`')
        
    def _backup(self, table_name):
            
            conn, curs = self._connect()
            
            table_list = self.get_tbl_list()
            if table_name in table_list:
                backup_table_name = f'{table_name}_bak_{self.today}'
                
                # 백업 테이블이 이미 존재하는경우 rename
                i = 1
                while backup_table_name in table_list:
                    backup_table_name = backup_table_name + f'_{i}'
                    i += 1
                    
                query = f'ALTER TABLE {table_name} RENAME {backup_table_name};'
                curs.execute(query)
            else:
                pass
            
            conn.commit()
            curs.close()
            conn.close()
        
    def create_table(self, upload_df, table_name, append=False):
        ''' Create table '''
            
        query_dict = {           
                      
            'dart_finstatements': f'CREATE TABLE `dart_finstatements` (\
                                    `rcept_no` varchar(20),\
                                    `reprt_code` varchar(20),\
                                    `corp_code` varchar(20),\
                                    `bsns_year` varchar(20),\
                                    `fs_div` varchar(20),\
                                    `sj_div` varchar(20),\
                                    `sj_nm` varchar(20),\
                                    `stock_code` varchar(20),\
                                    `account_id` varchar(255),\
                                    `account_nm` varchar(255),\
                                    `thstrm_nm` varchar(20),\
                                    `thstrm_amount` float DEFAULT NULL\
                                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;',
            
            'dart_amounts_all': f'CREATE TABLE `dart_amounts_all` (\
                                            `stock_code` varchar(20),\
                                            `corp_code` varchar(20),\
                                            `fs_div` varchar(20),\
                                            `sj_div` varchar(20),\
                                            `account_id` varchar(255),\
                                            `account_nm` text,\
                                            `Q202211013` float DEFAULT NULL,\
                                            `Y202111011` float DEFAULT NULL,\
                                            `Q202111014` float DEFAULT NULL,\
                                            `Q202111012` float DEFAULT NULL,\
                                            `Q202111013` float DEFAULT NULL,\
                                            `Y202011011` float DEFAULT NULL,\
                                            `Q202011014` float DEFAULT NULL,\
                                            `Q202011012` float DEFAULT NULL,\
                                            `Q202011013` float DEFAULT NULL,\
                                            `Y201911011` float DEFAULT NULL,\
                                            `Q201911014` float DEFAULT NULL,\
                                            `Q201911012` float DEFAULT NULL,\
                                            `Q201911013` float DEFAULT NULL,\
                                            `Y201811011` float DEFAULT NULL,\
                                            `Q201811014` float DEFAULT NULL,\
                                            `Q201811012` float DEFAULT NULL,\
                                            `Q201811013` float DEFAULT NULL\
                                            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;',
                                            
            'dart_amounts': f'CREATE TABLE `dart_amounts` (\
                                            `id` int(11) unsigned NOT NULL AUTO_INCREMENT,\
                                            `account_nm_eng` varchar(255),\
                                            `account_id` varchar(255),\
                                            `account_nm_kor` varchar(255),\
                                            `stock_code` varchar(20),\
                                            `fs_div` varchar(20),\
                                            `sj_div` varchar(20),\
                                            `Q202211013` float DEFAULT NULL,\
                                            `Q202111011` float DEFAULT NULL,\
                                            `Q202111014` float DEFAULT NULL,\
                                            `Q202111012` float DEFAULT NULL,\
                                            `Q202111013` float DEFAULT NULL,\
                                            `Q202011011` float DEFAULT NULL,\
                                            `Q202011014` float DEFAULT NULL,\
                                            `Q202011012` float DEFAULT NULL,\
                                            `Q202011013` float DEFAULT NULL,\
                                            `Q201911011` float DEFAULT NULL,\
                                            `Q201911014` float DEFAULT NULL,\
                                            `Q201911012` float DEFAULT NULL,\
                                            `Q201911013` float DEFAULT NULL,\
                                            `Q201811011` float DEFAULT NULL,\
                                            `Q201811014` float DEFAULT NULL,\
                                            `Q201811012` float DEFAULT NULL,\
                                            `Q201811013` float DEFAULT NULL,\
                                            PRIMARY KEY (`id`)\
                                            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;',
                                            
            'dart_annualized': f'CREATE TABLE `dart_annualized` (\
                                            `id` int(11) unsigned NOT NULL AUTO_INCREMENT,\
                                            `account_nm_eng` varchar(255),\
                                            `account_id` varchar(255),\
                                            `account_nm_kor` varchar(255),\
                                            `stock_code` varchar(20),\
                                            `fs_div` varchar(20),\
                                            `sj_div` varchar(20),\
                                            `Q202211013` float DEFAULT NULL,\
                                            `Q202111011` float DEFAULT NULL,\
                                            `Q202111014` float DEFAULT NULL,\
                                            `Q202111012` float DEFAULT NULL,\
                                            `Q202111013` float DEFAULT NULL,\
                                            `Q202011011` float DEFAULT NULL,\
                                            `Q202011014` float DEFAULT NULL,\
                                            `Q202011012` float DEFAULT NULL,\
                                            `Q202011013` float DEFAULT NULL,\
                                            `Q201911011` float DEFAULT NULL,\
                                            `Q201911014` float DEFAULT NULL,\
                                            `Q201911012` float DEFAULT NULL,\
                                            `Q201911013` float DEFAULT NULL,\
                                            `Q201811011` float DEFAULT NULL,\
                                            PRIMARY KEY (`id`)\
                                            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;',
                                            
            'dart_annualized_octa': f'CREATE TABLE `dart_annualized_octa` (\
                                            `id` int(11) unsigned NOT NULL AUTO_INCREMENT,\
                                            `account_nm_eng` varchar(255),\
                                            `account_id` varchar(255),\
                                            `account_nm_kor` varchar(255),\
                                            `stock_code` varchar(20),\
                                            `fs_div` varchar(20),\
                                            `sj_div` varchar(20),\
                                            `Q202211013` float DEFAULT NULL,\
                                            `Q202111011` float DEFAULT NULL,\
                                            `Q202111014` float DEFAULT NULL,\
                                            `Q202111012` float DEFAULT NULL,\
                                            `Q202111013` float DEFAULT NULL,\
                                            `Q202011011` float DEFAULT NULL,\
                                            `Q202011014` float DEFAULT NULL,\
                                            `Q202011012` float DEFAULT NULL,\
                                            `Q202011013` float DEFAULT NULL,\
                                            `Q201911011` float DEFAULT NULL,\
                                            PRIMARY KEY (`id`)\
                                            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;',
                                            
            'dart_ratios': f'CREATE TABLE `dart_ratios` (\
                                            `id` int(11) unsigned NOT NULL AUTO_INCREMENT,\
                                            `stock_code` varchar(20),\
                                            `ratio` varchar(255),\
                                            `Q202211013` float DEFAULT NULL,\
                                            `Q202111011` float DEFAULT NULL,\
                                            `Q202111014` float DEFAULT NULL,\
                                            `Q202111012` float DEFAULT NULL,\
                                            `Q202111013` float DEFAULT NULL,\
                                            `Q202011011` float DEFAULT NULL,\
                                            `Q202011014` float DEFAULT NULL,\
                                            `Q202011012` float DEFAULT NULL,\
                                            `Q202011013` float DEFAULT NULL,\
                                            `Q201911011` float DEFAULT NULL,\
                                            `Q201911014` float DEFAULT NULL,\
                                            `Q201911012` float DEFAULT NULL,\
                                            `Q201911013` float DEFAULT NULL,\
                                            `Q201811011` float DEFAULT NULL,\
                                            `Q201811014` float DEFAULT NULL,\
                                            `Q201811012` float DEFAULT NULL,\
                                            `Q201811013` float DEFAULT NULL,\
                                            PRIMARY KEY (`id`)\
                                            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;',
                                            
            'dart_accounts': f'CREATE TABLE `dart_accounts` (\
                                `id` int(11) unsigned NOT NULL AUTO_INCREMENT,\
                                `account_nm_eng` varchar(255),\
                                `account_id` varchar(255),\
                                `account_nm_kor` varchar(255),\
                                PRIMARY KEY (`id`)\
                                ) ENGINE=InnoDB DEFAULT CHARSET=utf8;',
                                        
            'dart_accounts_all': f'CREATE TABLE `dart_accounts_all` (\
                                    `sj_div` varchar(20),\
                                    `account_id` varchar(255),\
                                    `account_nm` text\
                                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;',
                                    
            'fnguide_ratio': f'CREATE TABLE `fnguide_ratio` (\
                                `stock_code` varchar(20),\
                                `ratio` varchar(255),\
                                `Y_4` float DEFAULT NULL,\
                                `Y_3` float DEFAULT NULL,\
                                `Y_2` float DEFAULT NULL,\
                                `Y_1` float DEFAULT NULL,\
                                `CLE` float DEFAULT NULL,\
                                `note` varchar(20)\
                                ) ENGINE=InnoDB DEFAULT CHARSET=utf8;',
        }
        
        if table_name in list(query_dict.keys()):
            query = query_dict[table_name]
        else:
            query = None
        
        if query == None:
            print('query is None')
        else:
            table_list = self.get_tbl_list()
            conn, curs = self._connect()
            if not append:
                # backup table
                self._backup(table_name)
            
                # create table
                curs.execute(query)
            else:
                if table_name in table_list:
                    pass
                else:
                    # create table
                    curs.execute(query)
            
            # upload table
            self.engine_upload(upload_df, table_name, if_exists_option='append')
            
            # drop temporary table
            if  f'{table_name}_temp' in table_list:
                curs.execute(f'DROP TABLE {table_name}_temp;')
            
            # commit & close
            conn.commit()
            curs.close()
            conn.close()        