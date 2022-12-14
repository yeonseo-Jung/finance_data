import os
import sys
from tqdm.auto import tqdm

try:
    from pandas import json_normalize
except ImportError:
    from pandas.io.json import json_normalize

import OpenDartReader
    
if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    root = sys._MEIPASS
else:
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    root = os.path.abspath(os.path.join(cur_dir, os.pardir))
    sys.path.append(root)

tbl_cache = os.path.join(root, 'tbl_cache')
conn_path = os.path.join(root, 'conn.txt')

from crawling.crawler import *
from config.consts import Dart

class CrawlingDart:
    
    def __init__(self):
        self.api_keys = ['ef3149d745caee09f48df5004b905ec4ef3f5d7e', '60bf63f4a8d7e074f83a32253045eba40971bd8c']
        self.key_idx = 1
        self.api_key = self.api_keys[0]
        self.dart = OpenDartReader(self.api_key)
        
        self.quarters = Dart.quarters
        # [
        #     'Q202211013', 
        #     'Y202111011', 'Q202111014', 'Q202111012', 'Q202111013',
        #     'Y202011011', 'Q202011014', 'Q202011012', 'Q202011013',
        #     'Y201911011', 'Q201911014', 'Q201911012', 'Q201911013',
        #     'Y201811011', 'Q201811014', 'Q201811012', 'Q201811013',
        # ]
    
    def add_quarter(self, quarter):
        self.quarters = [quarter] + self.quarters
        
    def get_finstate(self, stock_code, quarter):
        ''' Import Single Company Single Quarter Financial Statements '''
        
        kind = quarter[0]
        year = quarter[1:5]
        reprt_code = quarter[5:]
        status = 1
        '''status
            *  1: normal
            *  0: omission
            * -1: limit Excess
            * -2: unknown
        '''
        try:
            df = self.dart.finstate_all(corp=stock_code[0:6], bsns_year=year, reprt_code=reprt_code, fs_div='CFS')
            if df is None:
                df = self.dart.finstate_all(corp=stock_code[0:6], bsns_year=year, reprt_code=reprt_code, fs_div='OFS')
                if df is None:
                    # 재무제표 누락
                    status = 0
                else:
                    if len(df) == 0:
                        # 재무제표 누락
                        status = 0
                    else:
                        # 별도 재무제표
                        df.loc[:, 'stock_code'] = stock_code
                        df.loc[:, 'fs_div'] = "OFS"
                        df.loc[:, 'thstrm_nm'] = kind + str(year) + str(reprt_code)
            else:
                if len(df) == 0:
                    df = self.dart.finstate_all(corp=stock_code[0:6], bsns_year=year, reprt_code=reprt_code, fs_div='OFS')
                    if df is None:
                        # 재무제표 누락
                        status = 0
                    else:
                        if len(df) == 0:
                            # 재무제표 누락
                            status = 0
                        else:
                            # 별도 재무제표
                            df.loc[:, 'stock_code'] = stock_code
                            df.loc[:, 'fs_div'] = "OFS"
                            df.loc[:, 'thstrm_nm'] = kind + str(year) + str(reprt_code)
                else:
                    # 연결 재무제표
                    df.loc[:, 'stock_code'] = stock_code
                    df.loc[:, 'fs_div'] = "CFS"
                    df.loc[:, 'thstrm_nm'] = kind + str(year) + str(reprt_code)
                    
        except ValueError:
            # dart 조회 한도 초과
            status = -1         
            df = None
                
        except Exception as e:
            # print(f'\n\nError: {str(e)}\n\n')
            status = -2
            df = None
                
        return df, status
    
    def get_finstates(self, stock_code):
        ''' Import single company multi-quarter financial statements '''
        
        fins, error, error_none = [], [], []
        check_nan, cnt_nan, error_status = 0, 0, 1
        for q in self.quarters:
            fin, status = self.get_finstate(stock_code, q)
            
            if status == 1:
                fins.append(fin)
                check_nan == 0
                
            elif status == 0:
                error_none.append([stock_code, q])
                if check_nan == 0:
                    if cnt_nan == 0:
                        check_nan = 1
                        cnt_nan += 1
                    else:
                        error_status = 0
                else:
                    cnt_nan += 1
                    
            elif status == -1:
                print("Max Try Error!\nRe-Connect Dart")
                api_key = self.api_keys[self.key_idx]
                self.key_idx += 1
                self.dart = CrawlingDart(api_key)
                fin, status = self.get_finstate(stock_code, q)
            
            elif status == -2:
                error.append([stock_code, q])
                error_status = -1
                
        if error_status == 1:
            pass
        else:
            fins = []
            
        return fins, error, error_none