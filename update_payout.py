# ==========================================
# 📂 檔案名稱： update_finance.py (四道安全鎖防呆版)
# 💡 任務： 抓取 EPS + 股利，具備嚴格防呆機制，絕不覆蓋歷史資料
# ==========================================

import os
import requests
import gspread
from google.oauth2.service_account import Credentials
import urllib3
import json

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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

def fetch_and_update():
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    print("📡 下載最新【綜合損益表 EPS】...")
    try:
        res_twse = requests.get("https://openapi.twse.com.tw/v1/opendata/t187ap14_L", headers=headers, verify=False, timeout=30).json()
        res_tpex = requests.get("https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap14_O", headers=headers, verify=False, timeout=30).json()
        all_eps = res_twse + res_tpex
    except Exception as e: 
        print(f"❌ EPS 抓取失敗: {e}")
        all_eps = []

    stats = {}
    for item in all_eps:
        code = str(item.get('公司代號', item.get('co_id', ''))).strip()
        if not code: continue
        
        annual_eps = 0.0
        for k, v in item.items():
            if '每股盈餘' in k: 
                annual_eps = force_float(v)
                break
        
        stats[code] = {"annual_eps": annual_eps, "dividend": None}

    print("📡 下載最新【現金股利】...")
    try:
        res_twse_div = requests.get("https://openapi.twse.com.tw/v1/opendata/t187ap11_L", headers=headers, verify=False, timeout=30).json()
        res_tpex_div = requests.get("https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap11_O", headers=headers, verify=False, timeout=30).json()
        all_div = res_twse_div + res_tpex_div
        
        for item in all_div:
            code = str(item.get('公司代號', item.get('co_id', ''))).strip()
            if not code: continue
            
            div = 0.0
            for k, v in item.items():
                if '現金股利總計' in k or '股利合計' in k:
                    div = force_float(v)
                    break
            
            if code in stats:
                stats[code]["dividend"] = div
    except Exception as e:
        print(f"❌ 股利 抓取失敗: {e}")

    client = get_gspread_client()
    spreadsheet = client.open_by_url(MASTER_GSHEET_URL)
    
    for ws in spreadsheet.worksheets():
        if "當年度表" not in ws.title: continue
        data = ws.get_all_values()
        if not data: continue
        
        h = data[0]
        i_c = next((i for i, x in enumerate(h) if "代號" in x), -1)
        if i_c == -1: continue
        
        i_q1 = next((i for i, x in enumerate(h) if "25Q1單季每股盈餘" in str(x)), -1)
        i_q2 = next((i for i, x in enumerate(h) if "25Q2單季每股盈餘" in str(x)), -1)
        i_q3 = next((i for i, x in enumerate(h) if "25Q3單季每股盈餘" in str(x)), -1)
        
        i_q4_target = next((i for i, x in enumerate(h) if "25Q4單季每股盈餘" in str(x)), -1)
        i_accum_eps_target = next((i for i, x in enumerate(h) if "最新累季每股盈餘" in str(x)), -1)
        i_div_target = next((i for i, x in enumerate(h) if "合計股利" in str(x)), -1)
        i_payout_target = next((i for i, x in enumerate(h) if "盈餘總分配率" in str(x)), -1)

        cells = []
        for r_idx, row in enumerate(data[1:], start=2):
            code = row[i_c].split('.')[0].strip()
            
            if code in stats:
                d = stats[code]
                
                # 🔒 安全鎖 1：有 EPS 才更新
                if i_accum_eps_target != -1 and d["annual_eps"] != 0:
                    cells.append(gspread.Cell(row=r_idx, col=i_accum_eps_target+1, value=d["annual_eps"]))
                    
                # 🔒 安全鎖 2：有 EPS 才算 Q4，避免還沒公佈財報卻算出負數
                if i_q4_target != -1 and d["annual_eps"] != 0:
                    q1_eps = force_float(row[i_q1]) if i_q1 != -1 and i_q1 < len(row) else 0.0
                    q2_eps = force_float(row[i_q2]) if i_q2 != -1 and i_q2 < len(row) else 0.0
                    q3_eps = force_float(row[i_q3]) if i_q3 != -1 and i_q3 < len(row) else 0.0
                    q4_eps_calculated = round(d["annual_eps"] - q1_eps - q2_eps - q3_eps, 2)
                    cells.append(gspread.Cell(row=r_idx, col=i_q4_target+1, value=q4_eps_calculated))
                    
                # 🔒 安全鎖 3：股利大於 0 才寫，避免蓋掉您的歷史防守資料
                if i_div_target != -1 and d["dividend"] is not None and d["dividend"] > 0:
                    cells.append(gspread.Cell(row=r_idx, col=i_div_target+1, value=d["dividend"]))
                    
                # 🔒 安全鎖 4：股利大於 0 且有 EPS 才算最新配息率
                if i_payout_target != -1 and d["dividend"] is not None and d["dividend"] > 0 and d["annual_eps"] > 0:
                    payout = round((d["dividend"] / d["annual_eps"]) * 100, 2)
                    cells.append(gspread.Cell(row=r_idx, col=i_payout_target+1, value=payout))

        if cells:
            ws.update_cells(cells, value_input_option='USER_ENTERED')
            print(f"📊 {ws.title} 更新完成。寫入了 {len(cells)} 個儲存格。")

if __name__ == "__main__":
    fetch_and_update()
