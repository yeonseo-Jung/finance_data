# necessary
import os
import sys
import time
from datetime import datetime
import pandas as pd

# db connection 
import pymysql
import sqlalchemy

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    root = sys._MEIPASS
else:
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    root = os.path.abspath(os.path.join(cur_dir, os.pardir))
    sys.path.append(root)

from config.consts import Dart    
class AccessDataBase:
    
    def __init__(self, user_name, password, db_name):
        self.today = datetime.today().strftime('%y%m%d')
        
        # user info & db
        self.user_name = user_name
        self.password = password
        self.db_name = db_name
        self.host_url = "localhost"
        
    def _connect(self):
        ''' db connect '''
            
        port_num = 3306
        conn = pymysql.connect(host=self.host_url, user=self.user_name, passwd=self.password, port=port_num, db=self.db_name, charset='utf8')
        curs = conn.cursor(pymysql.cursors.DictCursor)
        
        return conn, curs
    
    def _execute(self, query):
        
        try:
            conn, curs = self._connect()
            curs.execute(query)
            data = curs.fetchall()
        except Exception as e:
            raise e
        finally:
            conn.commit()
            curs.close()
            conn.close()
        
        return data

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
        INSERT INTO `{table_name}`{_fields_} \
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
            
    def set_date(sale, df: pd.DataFrame) -> pd.DataFrame:
        
        df.loc[:, ["created", "updated"]] = pd.Timestamp(datetime.today()), pd.Timestamp(datetime.today())
        
        return df
        
    def create_table(self, upload_df, table_name, append=False):
        ''' Create table '''
            
        query_dict = Dart.query_dict
        
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