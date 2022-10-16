import requests
import pandas as pd
from bs4 import BeautifulSoup

def html_parser(url):
    
    # get url
    content = requests.get(url).content

    # parsing
    html = BeautifulSoup(content, 'html.parser')
    return html

def get_ratio(stock_code):
    
    stock_code = stock_code[0:6]
    url = requests.get(f'http://asp01.fnguide.com/SVO2/asp/SVD_FinanceRatio.asp?pGB=1&gicode=A{stock_code}')
    html = html_parser(url)
    
    if html is None:
        ratio_df = None
    else:
        table = html.find('table', 'us_table_ty1 h_fix zigbg_no')
        
        if table is None:
            ratio_df = None
        else:
            trs = table.find('tbody').find_all('tr')
            data = []
            for tr in trs:
                _clsss = ' '.join(tr['class']).strip()
                
                status = True
                if _clsss == 'rwf acd_dep_start_close':
                    note = 'ratio'
                elif _clsss == 'tbody_tit':
                    status = False
                else:
                    note = 'amounts'
                
                if status:
                    
                    # ratio or account name
                    th = tr.find('th')
                    if th.find('dt') is None:
                        name = th.text.strip()
                    else:
                        name = tr.find('th').find('dt').text.strip()
                    
                    
                    # ratio or amounts
                    amounts = [stock_code, name]
                    for am in tr.find_all('td', 'r'):
                        am = am.text.replace(',', '').strip()
                        if am == '':
                            amount = None
                        else:
                            amount = float(am)
                        amounts.append(amount)
                        
                    amounts.append(note)
                    data.append(amounts)

            columns = ['stock_code', 'ratio', 'Y201811011', 'Y201911011', 'Y202011011', 'Y202111011', 'Q202211012', 'note']
            ratio_df = pd.DataFrame(data, columns=columns)
            
    return ratio_df

def get_amounts(stock_code, finstate="cis_y"):
    
    finstate_dict = {
        "cis_y": "divSonikY",
        "cis_q": "divSonikQ",
        "bs_y": "divDaechaY",
        "bs_q": "divDaechaQ",
        "cfs_y": "divCashY",
        "cfs_q": "divCashQ",
    }
    finstate_kind = finstate_dict[finstate]
    
    url = f'https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A{stock_code}'
    html = html_parser(url)
    
    body = html.find('body')
    fn_body = body.find('div',{'class':'fng_body asp_body'})
    table = fn_body.find('div',{'id':f'{finstate_kind}'})

    tbody = table.find('tbody')
    tr = tbody.find_all('tr')

    # 기간 가져오기 
    # table columns 지정
    thead = table.find("thead")
    terms = thead.find("tr").find_all("th")

    columns_arr = []
    for q in terms:
        columns_arr.append(q.text)    
    columns_arr[0] = "Account"
    # columns 지정
    Table = pd.DataFrame(columns=columns_arr)
    
    index = 0
    for i in tr:

        # 계정과목이름 
        account_nm = i.find('span',{'class':'txt_acd'})

        if account_nm == None:
            account_nm = i.find('th')   

        account_nm = account_nm.text.strip()
        Table.loc[index, "Account"] = account_nm

        # 금액 
        value_list =[]

        values = i.find_all('td',{'class':'r'})

        for value in values:
            temp = value.text.replace(',','').strip()

            try:
                temp = float(temp)
                value_list.append(temp)
            except:
                value_list.append(0)

        Table.loc[index, columns_arr[1:]] = value_list
        
        index += 1
        
    return Table