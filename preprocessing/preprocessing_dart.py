import os
import sys
from datetime import datetime
import numpy as np
import pandas as pd
from tqdm.auto import tqdm

# Exception Error Handling
import warnings
warnings.filterwarnings("ignore")

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    root = sys._MEIPASS
else:
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    root = os.path.abspath(os.path.join(cur_dir, os.pardir))
    sys.path.append(root)

tbl_cache = os.path.join(root, 'tbl_cache')
conn_path = os.path.join(root, 'conn.txt')

from crawling.crawling_dart import CrawlingDart
from database.access import AccessDataBase
from config.consts import Dart
class DartFinstate:
    def __init__(self, dart_finstate):
        super().__init__()
        self.dart_finstate = dart_finstate
        self.quarters = Dart.quarters
        self.today = datetime.today()
        self.quarters_q = Dart.quarters_q
        
    def md_account_tbl(self):
        ''' Create account table '''

        accounts = self.dart_finstate.drop_duplicates(subset=['sj_div', 'account_id', 'account_nm'], keep='first').loc[:, ['sj_div', 'account_id', 'account_nm']]
        accounts.loc[accounts.account_id=='-표준계정코드 미사용-', 'account_id'] = None
        accounts_notnull = accounts[accounts.account_id.notnull()].reset_index(drop=True)
        accounts_isnull = accounts[accounts.account_id.isnull()].reset_index(drop=True)
        
        account_list = []
        finstates = accounts.sj_div.unique()
        for finstate in finstates:
            finstate_df = accounts_notnull.loc[accounts_notnull.sj_div==finstate].reset_index(drop=True)
            _finstate_df = accounts_isnull.loc[accounts_isnull.sj_div==finstate].reset_index(drop=True)
            
            account_ids = finstate_df.account_id.unique()
            for id_ in account_ids:
                nms = finstate_df.loc[finstate_df.account_id==id_, 'account_nm'].values.tolist()
                nms = list(set(nms))
                account_list.append([finstate, id_, str(nms)])
            
            account_nms = _finstate_df.account_nm.unique()
            for nm in account_nms:
                account_list.append([finstate, None, str(nm)])
                
        account_df = pd.DataFrame(account_list, columns=['sj_div', 'account_id', 'account_nm'])
        
        return account_df
    
    def find_account_id(self, corp_df, sj_div, account_id, account_nm, stock_code):
        ''' Find amount by account id '''
        
        # columns = ['stock_code', 'fs_div'] + corp_df.columns.tolist()[0:11]
        if account_id is None:
            acc_df = corp_df.loc[(corp_df.account_nm==account_nm) & (corp_df.sj_div==sj_div)].reset_index(drop=True)
        else:
            acc_df = corp_df.loc[(corp_df.account_id==account_id) & (corp_df.sj_div==sj_div)].reset_index(drop=True)
        
        if len(acc_df) == 0:
            row = None
        else:
            corp_code = acc_df.corp_code.tolist()[0]
            fs_div = acc_df.fs_div.tolist()[0]
            info = [stock_code, corp_code, fs_div, sj_div, account_id, account_nm]
            
            status = 1
            amounts = []
            try:
                for q in self.quarters:
                    amount = acc_df.loc[acc_df.thstrm_nm==q, 'thstrm_amount'].values
                    if len(amount) == 0:
                        # 해당 분기 재무제표가 존재하지 않는 경우 
                        amount = np.nan
                    elif len(amount) == 1:
                        amount = amount[0]
                    else:
                        # 계정과목 두개 이상
                        raise Exception(f'stock_code: {stock_code}\naccount_id: {account_id}\nsj_div: {sj_div} \naccount_nm: {account_nm}\nqaurter: {q}\nammount: {amount.tolist()}') 
                    amounts.append(amount)
            except Exception as e:
                status = 0
            
            if status == 1:
                row = info + amounts
            else:
                row = None
            
        return row
    
    def create_amount_quarter(self):
        ''' Create amount table '''
        
        account_df = self.md_account_tbl()

        # BS, IS, CIS, CF
        sj_divs = ['BS', 'CIS', 'IS', 'CF']
        _account_df = account_df[account_df.sj_div.isin(sj_divs)].reset_index(drop=True)

        stock_codes = self.dart_finstate.stock_code.unique()
        accounts = []
        for stock_code in tqdm(stock_codes):
            corp_df = self.dart_finstate.loc[self.dart_finstate.stock_code==stock_code].reset_index(drop=True)
            for idx in range(len(_account_df)):
                sj_div = _account_df.loc[idx, 'sj_div']
                account_id = _account_df.loc[idx, 'account_id']
                account_nm = _account_df.loc[idx, 'account_nm']
                amount = self.find_account_id(corp_df, sj_div, account_id, account_nm, stock_code)
                
                if amount is None:
                    continue
                else:
                    accounts.append(amount)
                    
        info_columns = ['stock_code', 'corp_code', 'fs_div', 'sj_div', 'account_id', 'account_nm']
        columns = info_columns + self.quarters
        amounts_all_df = pd.DataFrame(accounts, columns=columns)
        
        return amounts_all_df
    
    def calculate_quarter(self, df_accounts):
        ''' 4분기 순액 구하기 '''
        
        year = []
        for y in range(self.today.year-1, 2017, -1):
            year.append(str(y))
        reprts = ['11014', '11012', '11013']

        idx_range = range(len(df_accounts))
        for idx in tqdm(idx_range):
            df_account = df_accounts.loc[idx]
            sj_div = df_accounts.loc[idx, 'sj_div']
            for y in year:
                amount_y = df_account[f'Y{y}11011']
                if sj_div == 'IS' or sj_div == 'CIS' or sj_div == 'CF':
                    amount_qs = 0
                    validity = True
                    for reprt in reprts:
                        quarter = 'Q' + y + reprt
                        amount_q = df_account[quarter]
                        amount_qs += amount_q            
                    amount_4q = amount_y - amount_qs
                    
                elif sj_div == 'BS':
                    amount_4q = df_account[f'Y{y}11011']
                
                df_accounts.loc[idx, f'Q{y}11011'] = amount_4q
                
        info_columns = ['stock_code', 'fs_div', 'sj_div', 'account_id', 'account_nm_eng', 'account_nm_kor']
    
        columns = info_columns + self.quarters_q
        df_accounts = df_accounts.loc[:, columns]
        
        return df_accounts

    def get_amounts(self, amounts_all_df, accounts_df):
        ''' 특정 계정과목 금액 추출하기 '''
        
        account_list = accounts_df.account_nm_eng.unique().tolist()
        amount_list = []
        for stock_code in tqdm(amounts_all_df.stock_code.unique()):
            corp_df = amounts_all_df.loc[amounts_all_df.stock_code==stock_code]
            for nm in account_list:
                df = accounts_df.loc[accounts_df.account_nm_eng==nm]
                for account_id in df.account_id.unique():
                    if account_id in corp_df.loc[corp_df.sj_div=='IS'].account_id.unique():
                        _corp_df = corp_df[corp_df.sj_div=='IS']
                        break
                    elif account_id in corp_df.loc[corp_df.sj_div=='CIS'].account_id.unique():
                        _corp_df = corp_df[corp_df.sj_div=='CIS']
                        break
                    elif account_id in corp_df.loc[corp_df.sj_div=='BS'].account_id.unique():
                        _corp_df = corp_df[corp_df.sj_div=='BS']
                    elif account_id in corp_df.loc[corp_df.sj_div=='CF'].account_id.unique():
                        _corp_df = corp_df[corp_df.sj_div=='CF']
                        
                df_mer = df.merge(_corp_df, on='account_id', how='inner')
                if len(df_mer) == 0:
                    continue
                else:
                    check = True
                    validity = True
                    for amm in df_mer.loc[:, self.quarters].sum(min_count=1).tolist():
                        if str(amm) == 'nan':
                            check = False
                        else:
                            if check:
                                pass
                            else:
                                validity = False
                    
                    info_columns = ['stock_code', 'fs_div', 'sj_div', 'account_id', 'account_nm_eng', 'account_nm_kor']
                    amount_data = df_mer.loc[0, info_columns].tolist() + df_mer.loc[:, self.quarters].sum(min_count=1).tolist()
                    amount_data.append(validity)
                    amount_list.append(amount_data)
        
        columns = info_columns + self.quarters + ['validity']
        _amounts_df = pd.DataFrame(amount_list, columns=columns)
        
        return _amounts_df

db_yeonseo = AccessDataBase('root', 'jys9807!', 'yeonseo')
dartfins = DartFinstate(pd.DataFrame())
def update_amounts(accounts_df, amounts_all_df):
    ''' Get confirmed account amount 
        New data (append) '''

    accounts = db_yeonseo.get_tbl('accounts')
    confirmed_accounts = accounts.account_nm_eng.unique().tolist()
    confirmed_accounts_id = accounts.account_id.unique().tolist()

    new_accounts = []
    for account_id in accounts_df.account_id.unique():
        if account_id in confirmed_accounts_id:
            pass
        else:
            account = accounts_df.loc[accounts_df.account_id==account_id, "account_nm_eng"].values[0]
            new_accounts.append(account)
    accounts_new_df = accounts_df[accounts_df.account_nm_eng.isin(new_accounts)].reset_index(drop=True)

    if accounts_new_df.empty:
        status = 0
        accounts_new = None
        print('Dataframe empty')
    else:
        status = 1
        accounts_new = accounts_new_df.account_nm_eng.unique().tolist()
        amounts_df = dartfins.get_amounts(amounts_all_df, accounts_new_df)
        
    if status == 1:
        # Caculate 4th quarter net
        df_amounts_validitied = amounts_df.loc[amounts_df.validity].reset_index(drop=True)
        df_amounts_quarter = dartfins.calculate_quarter(df_amounts_validitied)
        accounts_new_df = accounts_new_df.drop(columns=["id"])
        
        # create date, regist date
        df_amounts_quarter.loc[:, 'created'] = pd.Timestamp(datetime.today().strftime("%Y-%m-%d"))
        df_amounts_quarter.loc[:, 'updated'] = pd.Timestamp(datetime.today().strftime("%Y-%m-%d"))
        accounts_new_df.loc[:, 'created'] = pd.Timestamp(datetime.today().strftime("%Y-%m-%d"))
        accounts_new_df.loc[:, 'updated'] = pd.Timestamp(datetime.today().strftime("%Y-%m-%d"))

        # Update table: amounts
        table = 'amounts'
        fields = tuple(df_amounts_quarter.columns.tolist())
        data = df_amounts_quarter.values.tolist()
        db_yeonseo.insert_many(table_name=table, fields=fields, data=data)

        # Update table: accounts
        db_yeonseo.engine_upload(upload_df=accounts_new_df, table_name="accounts", if_exists_option="append")
        
    return status, accounts_new

# def calculate_annualized(quarters_i=4):
#     ''' Calculate annualized amounts 
    
#     quarters_i = 4: 4분기 평균
#     quarters_i = 8: 8분기 평균
    
#     '''
    
#     df_amounts_quarter = db_yeonseo.get_tbl('amounts')
#     quarters=DartFinstate(pd.DataFrame()).quarters_q
    
#     info_amounts = []
#     n = range(len(df_amounts_quarter))
#     for idx in tqdm(n):
#         info = df_amounts_quarter.iloc[idx, 0:7].tolist()
#         sj_div = df_amounts_quarter.loc[idx, 'sj_div']
#         amounts = df_amounts_quarter.loc[idx, quarters]
        
#         _amounts = []
#         for i in range(len(amounts)-quarters_i+1):
#             amount = amounts[i:i+quarters_i].sum(skipna=False)
#             if sj_div == 'IS' or sj_div == 'CIS':
#                 amount = round(amount / quarters_i * 4, 0)
#             elif sj_div == 'BS':
#                 amount = round(amount / quarters_i, 0)
#             else:
#                 break
#             _amounts.append(amount)
        
#         info_amounts.append(info + _amounts)

#     columns = df_amounts_quarter.columns.tolist()[0:7] + quarters[:-quarters_i+1]
#     df_amounts_annual = pd.DataFrame(info_amounts, columns=columns)
    
#     return df_amounts_annual