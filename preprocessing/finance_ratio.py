import os
import re
import ast
import sys
import time
import pickle
from datetime import datetime
import numpy as np
import pandas as pd
from tqdm.auto import tqdm

# Exception Error Handling
import socket
import warnings
warnings.filterwarnings("ignore")

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    root = sys._MEIPASS
else:
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    root = os.path.abspath(os.path.join(cur_dir, os.pardir))
    sys.path.append(root)

from database.access import AccessDataBase

class GetRatio:
    def __init__(self):
        self.initDb()
        self.amounts_df = self.db.get_tbl('dart_amounts')
        self.allow_nulls = ['non_controlling_equity', 'non_controlling_profit']
        
    def initDb(self):
        self.db = AccessDataBase('root', 'jys1013011!', 'dart')
        
    def get_opts(self, stock_code, expressions):
        corp_df = self.amounts_df.loc[self.amounts_df.stock_code==stock_code]
        
        status = 1
        i = 0
        expressions_d = {}
        accounts, operators = [], []
        for exp in expressions:
            if i % 2 == 0:
                exp_df = corp_df.loc[corp_df.account_nm_eng==exp]
                if len(exp_df) == 0:
                    if exp in self.allow_nulls:
                        accounts.append(exp)
                        length = len(corp_df.iloc[:, 7:].columns)
                        expressions_d[exp] = np.array([0] * length)
                    else:
                        status = 0
                else:
                    accounts.append(exp)
                    expressions_d[exp] = exp_df.iloc[:, 7:].values[0]
            else:
                operators.append(exp)
            i += 1
        
        return expressions_d, accounts, operators, status
    
    def get_ratio(self, expressions_d, accounts, operators):
        _expressions_d = {}
        _accounts, _operators = [], []
        i = 0
        logs = []
        for account in accounts:
            val = expressions_d[account]
            if len(logs) == 0:
                logs.append(val)
            else:
                opt = operators[i-1]
                
                if len(opt) == 1:
                    if opt == '+':
                        _accounts.append(account)
                        _expressions_d[account] = logs[0]
                        _operators.append('++')
                    elif opt == '-':
                        _accounts.append(account)
                        _expressions_d[account] = logs[0]
                        _operators.append('--')
                    elif opt == '*':
                        cal = logs[0] * val
                        logs[0] = cal
                    elif opt == '/':
                        cal = logs[0] / val
                        logs[0] = cal
                    else:
                        raise Exception('Arithmetic Syntax Error')
                    
                elif len(opt) == 2:
                    if opt == '++':
                        cal = logs[0] + val
                        logs[0] = cal
                    elif opt == '--':
                        cal = logs[0] - val
                        logs[0] = cal
                    else:
                        raise Exception('Arithmetic Syntax Error')
            i += 1
            
        return logs, _expressions_d, _accounts, _operators
    
    def get_ratios(self, ratio, expressions):
        
        stock_codes = self.amounts_df.stock_code.unique()
        ratios, errors = [], []
        for stock_code in tqdm(stock_codes):
            # get operators
            expressions_d, accounts, operators, status = self.get_opts(stock_code, expressions)
            
            if status == 1:
                # get ratios
                logs, _expressions_d, _accounts, _operators = self.get_ratio(expressions_d, accounts, operators)
                while len(_accounts) != 0:
                    logs, _expressions_d, _accounts, _operators = self.get_ratio(_expressions_d, _accounts, _operators)
                
                ratios.append([stock_code, ratio] + logs[0].tolist())
            else:
                errors.append(stock_code)    
                
        return ratios, errors