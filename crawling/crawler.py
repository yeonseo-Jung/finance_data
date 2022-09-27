import requests as re
from bs4 import BeautifulSoup
import json
import numpy as np
import pandas as pd
from io import BytesIO
from tqdm import tqdm
import time
from datetime import datetime, timedelta

# 한국거래소(KRX) 웹사이트에서 전종목 정보 크롤링 함수 
def get_stock_info(market, date):    # market: kospi or kosdaq or konex    # date: ex) 20211001
    # Request URL
    url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
    # Form Data
    parms = {
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT01501',
        'share': '1',
        'money': '1',
        'csvxls_isNo': 'false',
    }

    if market == "kospi":
        parms['mktId'] = 'STK'
    elif market == "kosdaq":
        parms['mktId'] = 'KSQ'
    elif market == "konex":
        parms['mktId'] = "KNX"
        
    # 날짜 정보
    parms['trdDd'] = date
    
    # Request Headers ()
    headers = {
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020201',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36',
    }

    r = re.get(url, parms, headers=headers)

    jo = json.loads(r.text)
    df = pd.DataFrame(jo['OutBlock_1'])
    
    # 크롤링 한 데이터 테이블에서 필요한 정보만 추출하고 컬럼 명 변경해서 데이터프레임에 할당
    columns = ["종목코드", "종목명", "시장구분", "종가", "시가", "고가", "저가", "거래량", "거래대금", "시가총액", "상장주식수"]
    data = df[["ISU_SRT_CD", "ISU_ABBRV", "MKT_NM", "TDD_CLSPRC", "TDD_OPNPRC", "TDD_HGPRC", "TDD_LWPRC", "ACC_TRDVOL", "ACC_TRDVAL", "MKTCAP", "LIST_SHRS"]]
    stock_info_df = pd.DataFrame(columns=columns)
    stock_info_df[columns] = data
    
    # 금액 및 주식 수 데이터를 계산 가능하도록 콤마(,)제거하고 실수형 데이터로 변경하기
    i = 0
    while i < len(stock_info_df):
        col = stock_info_df.columns
        j = 3
        srs = stock_info_df.loc[i, ["종가", "시가", "고가", "저가", "거래량", "거래대금", "시가총액", "상장주식수"]]
        for data in srs:
            stock_info_df.loc[i, col[j]] = float(data.replace(",", ""))
            j += 1
        i += 1
        
    if market == 'kospi':
        stock_info_df.iloc[:, 0] = stock_info_df.iloc[:, 0] + ".KS"
    elif market == 'kosdaq':
        stock_info_df.iloc[:, 0] = stock_info_df.iloc[:, 0] + ".KQ"
    
    return stock_info_df

# 한국거래소(KRX) 웹사이트에서 보통주 정보 크롤링 함수 
def get_common_stock_info(market='kospi'):    # market: kospi or kosdaq or konex  
    # Request URL
    url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
    # Form Data
    parms = {
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT01901',
        'share': '1',
        'csvxls_isNo': 'false',
    }

    if market == "kospi":
        parms['mktId'] = 'STK'
    elif market == "kosdaq":
        parms['mktId'] = 'KSQ'
    elif market == "konex":
        parms['mktId'] = "KNX"
        
    # Request Headers ()
    headers = {
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020201',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36',
    }

    r = re.get(url, parms, headers=headers)

    jo = json.loads(r.text)
    df = pd.DataFrame(jo['OutBlock_1'])
    
    # 종목 정보 테이블에서 보통주만 추출하기 
    df_common = pd.DataFrame()
    i = 0
    j = 0
    while i < len(df):
        if df.loc[i, "KIND_STKCERT_TP_NM"] == "보통주":
            df_common.loc[j, ["종목코드", "종목명"]] = list(df.loc[i, ["ISU_SRT_CD", "ISU_ABBRV"]])
            j += 1
        i += 1
    
    if market == 'kospi':
        df_common.iloc[:, 0] = df_common.iloc[:, 0] + ".KS"
    elif market == 'kosdaq':
        df_common.iloc[:, 0] = df_common.iloc[:, 0] + ".KQ"
    
    return df_common