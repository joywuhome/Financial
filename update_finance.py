# ==========================================
# 📂 檔案名稱： update_finance.py (雙效全能版 - 專屬對應 API 精準校準版)
# 💡 任務： 每日自動更新【EPS + 算Q4】以及【全市場最新收盤價】！
# ==========================================

import os
import requests
import gspread
from google.oauth2.service_account import Credentials
import urllib3
import json

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 🌟 已經為您換回「精準校準版 (API)」的專屬 Google 表單網址！
MASTER_GSHEET_URL = "https://docs.google.com/spreadsheets/d/1vsqhH2i8aoRnBwPJ4BJ1eL2vQYGCkqabgG08f8P2A2c/edit"

def get_gspread_client():
    key_data = os.environ.get("GOOGLE_CREDENTIALS")
    if not key_data: raise ValueError("找不到 Google 金鑰")
    creds_dict = json.loads(key_data)
    creds = Credentials.from_service_account_info(creds_dict, scopes=['https://www.googleapis.com/auth/spreadsheets'])
    return gspread.authorize(creds)

def force_float(v):
    if v is None or str(v).strip() == "": return 0.0
    s = str(v).strip().replace(',', '')
    if s.startswith('(') and s.endswith(')'): s = '-' + s[1:-1]
    try: return float(s)
    except: return 0.0

def safe_parse_price(val):
    try:
        s = str(val).replace(',', '').strip()
        if not s or s == '-' or s == '--' or s == '---': return None
        return float(s)
    except: return None

def fetch_and_update():
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    # ---------------------------------------------------------
    # 任務一：抓取 EPS
    # ---------------------------------------------------------
    print("📡 任務一：下載最新【綜合損益表 EPS】...")
    try:
        res_twse_eps = requests.get("https://openapi.twse.com.tw/v1/opendata/t187ap14_L", headers=headers, verify=False, timeout=30).json()
        res_tpex_eps = requests.get("https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap14_O", headers=headers, verify=False, timeout=30).json()
        all_eps = res_twse_eps + res_tpex_eps
    except Exception as e: 
        print(f"❌ EPS 抓取失敗: {e}")
        all_eps = []

    stats = {}
    for item in all_eps:
        code = str(item.get('公司代號', item.get('co_id', ''))).strip()
        if not code: continue
        for k, v in item.items():
            if '每股盈餘' in k: 
                stats[code] = {"annual_eps": force_float(v)}
                break

    # ---------------------------------------------------------
    # 任務二：抓取盤後股價
    # ---------------------------------------------------------
    print("📡 任務二：下載最新【盤後收盤價】...")
    price_dict = {}
    try:
        res_twse_price = requests.get("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL", headers=headers, verify=False, timeout=30).json()
        if isinstance(res_twse_price, list):
            for i in res_twse_price:
                cp = safe_parse_price(i.get('ClosingPrice'))
                if cp is not None: price_dict[str(i.get('Code', '')).strip()] = cp
    except Exception as e: print(f"⚠️ 台灣證交所股價抓取失敗: {e}")

    try:
        res_tpex_price = requests.get("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes", headers=headers, verify=False, timeout=30).json()
        if isinstance(res_tpex_price, list):
            for i in res_tpex_price:
                cp = safe_parse_price(i.get('Close'))
                if cp is not None: price_dict[str(i.get('SecuritiesCompanyCode', '')).strip()] = cp
    except Exception as e: print(f"⚠️ 櫃買中心股價抓取失敗: {e}")
    
    print(f"✅ 成功抓取 {len(price_dict)} 檔股價報價。")

    # ---------------------------------------------------------
    # 任務三：開始寫入 Google Sheet
    # ---------------------------------------------------------
    print("📝 任務三：開始寫入 Google 表單...")
    client = get_gspread_client()
    spreadsheet = client.open_by_url(MASTER_GSHEET_URL)
    
    target_sheets = [ws for ws in spreadsheet.worksheets() if any(n in ws.title for n in ["當年度表", "個股總表", "總表", "金融股"])]
    
    for ws in target_sheets:
        data = ws.get_all_values()
        if not data: continue
        
        h = data[0]
        # 定位所有欄位
        c_idx = next((i for i, x in enumerate(h) if str(x).strip() in ["代號", "股票代號", "證券代號"]), -1)
        p_idx = next((i for i, x in enumerate(h) if str(x).strip() in ["成交", "股價", "最新股價", "收盤價"]), -1)
        
        i_q1 = next((i for i, x in enumerate(h) if "25Q1單季每股盈餘" in str(x).replace(" ", "")), -1)
        i_q2 = next((i for i, x in enumerate(h) if "25Q2單季每股盈餘" in str(x).replace(" ", "")), -1)
        i_q3 = next((i for i, x in enumerate(h) if "25Q3單季每股盈餘" in str(x).replace(" ", "")), -1)
        i_q4_target = next((i for i, x in enumerate(h) if "25Q4單季每股盈餘" in str(x).replace(" ", "")), -1)
        i_accum_eps_target = next((i for i, x in enumerate(h) if "最新累季每股盈餘" in str(x).replace(" ", "")), -1)

        if c_idx == -1: continue

        cells = []
        for r_idx, row in enumerate(data[1:], start=2):
            if c_idx >= len(row): continue
            code = str(row[c_idx]).split('.')[0].strip()
            
            # 寫入股價 (所有表單都寫)
            if p_idx != -1 and code in price_dict:
                cells.append(gspread.Cell(row=r_idx, col=p_idx+1, value=price_dict[code]))

            # 寫入 EPS (只在有這些欄位的表單寫)
            if code in stats:
                d = stats[code]
                if i_accum_eps_target != -1 and d["annual_eps"] != 0:
                    cells.append(gspread.Cell(row=r_idx, col=i_accum_eps_target+1, value=d["annual_eps"]))
                    
                if i_q4_target != -1 and d["annual_eps"] != 0:
                    q1_eps = force_float(row[i_q1]) if i_q1 != -1 and i_q1 < len(row) else 0.0
                    q2_eps = force_float(row[i_q2]) if i_q2 != -1 and i_q2 < len(row) else 0.0
                    q3_eps = force_float(row[i_q3]) if i_q3 != -1 and i_q3 < len(row) else 0.0
                    q4_eps_calculated = round(d["annual_eps"] - q1_eps - q2_eps - q3_eps, 2)
                    cells.append(gspread.Cell(row=r_idx, col=i_q4_target+1, value=q4_eps_calculated))

        if cells:
            ws.update_cells(cells, value_input_option='USER_ENTERED')
            print(f"📊 {ws.title} 更新完成。共寫入 {len(cells)} 個儲存格！")

if __name__ == "__main__":
    fetch_and_update()
