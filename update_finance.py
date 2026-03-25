# ==========================================
# 📂 檔案名稱： update_finance.py (精準校準版 專屬五效全能機器人)
# 💡 任務： 每日自動更新 EPS/Q4 + 股價 + PBR/PER/殖利率
# ⚠️ 修正： 嚴格鎖定「每股盈餘」字眼，絕不誤扣「單季營收」導致負數！
# ==========================================

import os
import requests
import gspread
from google.oauth2.service_account import Credentials
import urllib3
import json

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 🌟 精準校準版的專屬 Google Sheet 網址
MASTER_GSHEET_URL = "https://docs.google.com/spreadsheets/d/1vsqhH2i8aoRnBwPJ4BJ1eL2vQYGCkqabgG08f8P2A2c/edit"

def get_gspread_client():
    key_data = os.environ.get("GOOGLE_CREDENTIALS") or os.environ.get("GOOGLE_KEY_JSON")
    if not key_data: raise ValueError("找不到 Google 金鑰，請檢查 GitHub Secrets 設定！")
    creds_dict = json.loads(key_data)
    creds = Credentials.from_service_account_info(creds_dict, scopes=['https://www.googleapis.com/auth/spreadsheets'])
    return gspread.authorize(creds)

def force_float(v):
    if v is None or str(v).strip() in ["", "-", "--", "---", "N/A", "NaN"]: return 0.0
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
    # 任務一：抓取 EPS 與 四大財報指標
    # ---------------------------------------------------------
    try:
        print("📡 任務一：下載最新【綜合損益表】...")
        res_twse = requests.get("https://openapi.twse.com.tw/v1/opendata/t187ap14_L", headers=headers, verify=False, timeout=30).json()
        res_tpex = requests.get("https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap14_O", headers=headers, verify=False, timeout=30).json()
        all_detail = res_twse + res_tpex
    except Exception as e: 
        print(f"❌ API 抓取失敗: {e}")
        all_detail = []

    stats = {}
    for item in all_detail:
        code = str(item.get('公司代號', item.get('co_id', ''))).strip()
        if not code: continue
        y = str(item.get('年度'))
        q = str(item.get('季別'))
        
        if y == "114" and q == "4":
            revenue, op_profit, non_op_income, annual_eps = 0.0, 0.0, 0.0, 0.0
            for k, v in item.items():
                if '營業收入' in k: revenue = force_float(v)
                elif '營業利益' in k: op_profit = force_float(v)
                elif '營業外收入' in k: non_op_income = force_float(v)
                elif '每股盈餘' in k: annual_eps = force_float(v)

            stats[code] = {
                "annual_eps": annual_eps, 
                "revenue": revenue,
                "op_profit": op_profit,
                "non_op_income": non_op_income
            }

    # ---------------------------------------------------------
    # 任務二：抓取 盤後股價 與 PBR/PER/殖利率
    # ---------------------------------------------------------
    print("\n📡 任務二：下載最新【盤後數據與估值 (股價/PBR/PER/殖利率)】...")
    market_data = {}
    
    try:
        res_twse_price = requests.get("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL", headers=headers, verify=False, timeout=30).json()
        if isinstance(res_twse_price, list):
            for i in res_twse_price:
                c = str(i.get('Code', '')).strip()
                p = safe_parse_price(i.get('ClosingPrice'))
                if c and p is not None: market_data.setdefault(c, {})['price'] = p
    except: pass

    try:
        res_tpex_price = requests.get("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes", headers=headers, verify=False, timeout=30).json()
        if isinstance(res_tpex_price, list):
            for i in res_tpex_price:
                c = str(i.get('SecuritiesCompanyCode', '')).strip()
                p = safe_parse_price(i.get('Close'))
                if c and p is not None: market_data.setdefault(c, {})['price'] = p
    except: pass

    try:
        res_twse_val = requests.get("https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_ALL", headers=headers, verify=False, timeout=30).json()
        if isinstance(res_twse_val, list):
            for i in res_twse_val:
                c = str(i.get('Code', '')).strip()
                if c:
                    market_data.setdefault(c, {})['yield'] = force_float(i.get('Yield'))
                    market_data.setdefault(c, {})['per'] = force_float(i.get('PEratio'))
                    market_data.setdefault(c, {})['pbr'] = force_float(i.get('PBratio'))
    except: pass

    try:
        res_tpex_val = requests.get("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_perpeild", headers=headers, verify=False, timeout=30).json()
        if isinstance(res_tpex_val, list):
            for i in res_tpex_val:
                c = str(i.get('SecuritiesCompanyCode', '')).strip()
                if c:
                    market_data.setdefault(c, {})['yield'] = force_float(i.get('YieldRatio'))
                    market_data.setdefault(c, {})['per'] = force_float(i.get('PERatio'))
                    market_data.setdefault(c, {})['pbr'] = force_float(i.get('PBRatio'))
    except: pass

    # ---------------------------------------------------------
    # 任務三：開始寫入 Google 表單
    # ---------------------------------------------------------
    print("\n📝 任務三：開始寫入 Google 表單...")
    try:
        client = get_gspread_client()
        spreadsheet = client.open_by_url(MASTER_GSHEET_URL)
    except Exception as e:
        print(f"❌ Google 表單連線失敗: {e}")
        return
    
    target_sheets = [ws for ws in spreadsheet.worksheets() if any(n in ws.title for n in ["當年度表", "個股總表", "總表", "金融股"])]
    
    for ws in target_sheets:
        data = ws.get_all_values()
        if not data: continue
        
        h = data[0]
        i_c = next((i for i, x in enumerate(h) if str(x).strip() in ["代號", "股票代號", "證券代號"]), -1)
        i_price = next((i for i, x in enumerate(h) if str(x).strip() in ["成交", "股價", "最新股價", "收盤價"]), -1)
        
        i_pbr = next((i for i, x in enumerate(h) if "淨值比" in str(x) or "PBR" in str(x).upper()), -1)
        i_per = next((i for i, x in enumerate(h) if "本益比" in str(x) or "PER" in str(x).upper()), -1)
        i_yield = next((i for i, x in enumerate(h) if "殖利率" in str(x) and "年化" not in str(x)), -1)
        
        # 🌟 核心修復：嚴格鎖定「單季每股盈餘」，絕對不能去抓到「單季營收」！
        i_q1 = next((i for i, x in enumerate(h) if "25Q1" in str(x).upper() and "盈餘" in str(x)), -1)
        i_q2 = next((i for i, x in enumerate(h) if "25Q2" in str(x).upper() and "盈餘" in str(x)), -1)
        i_q3 = next((i for i, x in enumerate(h) if "25Q3" in str(x).upper() and "盈餘" in str(x)), -1)
        
        i_op_m_target = next((i for i, x in enumerate(h) if "最新單季營益率" in str(x)), -1)
        i_q4_target = next((i for i, x in enumerate(h) if "25Q4單季每股盈餘" in str(x)), -1)
        i_accum_eps_target = next((i for i, x in enumerate(h) if "最新累季每股盈餘" in str(x)), -1)
        i_nop_target = next((i for i, x in enumerate(h) if "最新單季業外損益佔稅前淨利" in str(x)), -1)
        
        if i_c == -1: continue

        cells = []
        for r_idx, row in enumerate(data[1:], start=2):
            if i_c >= len(row): continue
            code = str(row[i_c]).split('.')[0].strip()
            
            # 【寫入 市場資料】
            if code in market_data:
                m = market_data[code]
                if i_price != -1 and m.get('price', 0) > 0:
                    cells.append(gspread.Cell(row=r_idx, col=i_price+1, value=m['price']))
                if i_pbr != -1 and m.get('pbr', 0) > 0:
                    cells.append(gspread.Cell(row=r_idx, col=i_pbr+1, value=m['pbr']))
                if i_per != -1 and m.get('per', 0) > 0:
                    cells.append(gspread.Cell(row=r_idx, col=i_per+1, value=m['per']))
                if i_yield != -1 and m.get('yield', 0) > 0:
                    cells.append(gspread.Cell(row=r_idx, col=i_yield+1, value=m['yield']))
            
            # 【寫入 財報資料】
            if code in stats:
                d = stats[code]
                if i_accum_eps_target != -1:
                    cells.append(gspread.Cell(row=r_idx, col=i_accum_eps_target+1, value=d["annual_eps"]))
                if i_q4_target != -1:
                    # 這裡只會抓到真正的 EPS 去扣除了，不會再拿營收來扣！
                    q1_eps = force_float(row[i_q1]) if i_q1 != -1 and i_q1 < len(row) else 0.0
                    q2_eps = force_float(row[i_q2]) if i_q2 != -1 and i_q2 < len(row) else 0.0
                    q3_eps = force_float(row[i_q3]) if i_q3 != -1 and i_q3 < len(row) else 0.0
                    q4_eps_calculated = round(d["annual_eps"] - q1_eps - q2_eps - q3_eps, 2)
                    cells.append(gspread.Cell(row=r_idx, col=i_q4_target+1, value=q4_eps_calculated))
                if d["revenue"] != 0 and i_op_m_target != -1:
                    op_margin = round((d["op_profit"] / d["revenue"]) * 100, 2)
                    cells.append(gspread.Cell(row=r_idx, col=i_op_m_target+1, value=op_margin))
                pre_tax_profit = d["non_op_income"] + d["op_profit"]
                if pre_tax_profit != 0 and i_nop_target != -1:
                    non_op_ratio = round((d["non_op_income"] / pre_tax_profit) * 100, 2)
                    cells.append(gspread.Cell(row=r_idx, col=i_nop_target+1, value=non_op_ratio))
        
        if cells:
            ws.update_cells(cells, value_input_option='USER_ENTERED')
            print(f"📊 {ws.title} 更新完成。共寫入了 {len(cells)} 個儲存格 (含估值與財報)。")

if __name__ == "__main__":
    fetch_and_update()
