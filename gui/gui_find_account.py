import os
import sys
import pickle
import pandas as pd

from PyQt5 import uic
# from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QApplication
from PyQt5.QtWidgets import QMainWindow, QMessageBox, QHeaderView

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    root = sys._MEIPASS
else:
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    root = os.path.abspath(os.path.join(cur_dir, os.pardir))
    sys.path.append(root)

tbl_cache = os.path.join(root, 'tbl_cache')
conn_path = os.path.join(root, 'conn.txt')

from gui.table_viewer import DataFrameModel
from database.access import AccessDataBase
from preprocessing.preprocessing_dart import update_amounts

form_path = os.path.join(cur_dir, 'form', 'FindAccount.ui')
form = uic.loadUiType(form_path)[0]

class FindAccount(QMainWindow, form):
    ''' Product Status, Store Crawling Window '''
    
    def __init__(self):
        super().__init__()    
        self.setupUi(self)
        self.setWindowTitle('Find Accounts')
        
        # db 연결
        # with open(conn_path, 'rb') as f:
        #     conn = pickle.load(f)
        self.db_dart = AccessDataBase('root', 'jys1013011!', 'dart')
        self.db_dart_bak = AccessDataBase('root', 'jys1013011!', 'dart_bak')
        
        # data load
        self._load()
        # init ui
        self._initAccounts()
        
        # connect function
        self.search_account.clicked.connect(self._search_account)
        self.search_amount.clicked.connect(self._search_amount)
        self.Insert.clicked.connect(self._insert)
        self.Update.clicked.connect(self._update)
        
    def _initTable(self, TableView):
        header = TableView.horizontalHeader()
        
        for column in range(header.count()):
            header.setSectionResizeMode(column, QHeaderView.ResizeToContents)
    
    def _table_viewer(self, df, TableView):
        model = DataFrameModel(df)
        TableView.setModel(model)
        self._initTable(TableView)
        
    def _load(self):
        ''' Load Data '''
        
        # finstatements all
        sj_divs = ['BS', 'IS', 'CIS', 'CF']
        fins_df = self.db_dart_bak.get_tbl('dart_finstatements') 
        self.fins_df = fins_df.loc[fins_df.sj_div.isin(sj_divs)].reset_index(drop=True)
        
        # amounts all 
        amounts_all_df = self.db_dart_bak.get_tbl('dart_amounts_all')
        self.amounts_all_df = amounts_all_df.loc[amounts_all_df.account_id.notnull()].reset_index(drop=True)
        
    def _initAccounts(self):
        self.accounts_df = self.db_dart_bak.get_tbl('dart_accounts')
        self._table_viewer(self.accounts_df, self.ConfirmAccountTable)
        
    def _search_account(self):
        
        ac_id = self.account_id.text().replace(' ', '')
        ac_nm = self.account_nm.text().replace(' ', '')
        
        columns = ['sj_div', 'account_id', 'account_nm']
        if ac_id == '':
            _acc_df_id = pd.DataFrame(columns=columns)
        else:
            _acc_df_id = self.fins_df[self.fins_df.account_id.str.lower().str.contains(ac_id.lower(), na=False)]

        if ac_nm == '':
            _acc_df_nm = pd.DataFrame(columns=columns)
        else:
            _acc_df_nm = self.fins_df[self.fins_df.account_nm.str.contains(ac_nm, na=False)]

        acc_df_searched = pd.concat([_acc_df_nm, _acc_df_id]).loc[:, columns]
        acc_df_searched_dedup = acc_df_searched.drop_duplicates(keep='first').sort_values(by=columns, ignore_index=True)
        
        if acc_df_searched_dedup.empty:
            msg = QMessageBox()
            msg.setText('** No results found **')
            msg.exec_()
        else:
            self._table_viewer(acc_df_searched_dedup, self.AccountTable)
            
    def _search_amount(self):
        
        account_id = self.full_account_id.text().replace(' ', '')
        # stock_code = self.account_id.text().replace(' ', '')

        columns = ['stock_code', 'account_id', 'account_nm'] + self.amounts_all_df.columns.tolist()[6:10]
        amount_df_searched = self.amounts_all_df.loc[self.amounts_all_df.account_id==account_id, columns].reset_index(drop=True)
        
        if amount_df_searched.empty:
            msg = QMessageBox()
            msg.setText('** No results found **')
            msg.exec_()
        else:
            self._table_viewer(amount_df_searched, self.AmountTable)
            
    def _insert(self):
        ''' Insert confirmed account id & nm '''
        
        acc_nm_eng = self.account_nm_eng.text().replace(' ', '')
        acc_id = self.Account_id.text().replace(' ', '')
        acc_nm_kor = self.account_nm_kor.text().replace(' ', '')
        
        table = 'dart_accounts'
        field = ('account_nm_eng', 'account_id', 'account_nm_kor')
        values = (acc_nm_eng, acc_id, acc_nm_kor)
        
        if acc_id in self.accounts_df.account_id.unique():
            exist_data = self.accounts_df[self.accounts_df.account_id==acc_id].values[0].tolist()
            msg = QMessageBox()
            msg.setText(f'** Data that already exists **\n{exist_data}')
            msg.exec_()
        elif self.msg_event(values):
            self.db_dart_bak.insert(table, field, values)
            msg = QMessageBox()
            msg.setText('** Insert Data Successful **')
            msg.exec_()
            self._initAccounts()
        else:
            pass
        
    def _update(self):
        ''' update dart_amounts '''
        
        status, accounts_new = update_amounts(self.accounts_df, self.amounts_all_df)
        if status == 1:
            msg = QMessageBox()
            msg.setText(f'** Update Data Successful **\ntable_name: `dart_amounts`\ninserted data: {accounts_new}')
            msg.exec_()
        else:
            msg = QMessageBox()
            msg.setText('** No data to update **')
            msg.exec_()    
        
    def msg_event(self, values):
        info = QMessageBox.question(
            self, "Question", 
            f"Do you want to insert data?\n\
            - account_nm_eng: {values[0]}\n\
            - account_id: {values[1]}\n\
            - account_nm_kor: {values[2]}",
            QMessageBox.Yes | QMessageBox.No, 
            QMessageBox.Yes
        )
        if info == QMessageBox.Yes:
            return True
        elif info == QMessageBox.No:
            return False
        else:
            return False
    
if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = FindAccount()
    window.show()
    window.showFullScreen()
    app.exec_()