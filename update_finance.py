import os
import json
import time
import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# ==========================================
# 1. 系統設定與金鑰驗證
# ==========================================
# 請確認您的 Google Sheet 總表網址正確
MASTER_GSHEET_URL = "https://docs.google.com/spreadsheets/d/1uwAcFVjW0s6VKwkqU7tjptdkmFH6E0up/edit?gid=1314568005#gid=1314568005"

def get_gspread_client():
    """讀取 GitHub Secrets 保險箱裡的金鑰"""
    key_data = os.environ.get("GOOGLE_CREDENTIALS")
    if not key_data:
        raise ValueError("找不到 Google 金鑰環境變數，請檢查 GitHub Secrets 設定！")
    
    creds_dict = json.loads(key_data)
    creds = Credentials.from_service_account_info(creds_dict, scopes=['https://www.googleapis.com/auth/spreadsheets'])
    return gspread.authorize(creds)

# ==========================================
# 2. 官方資料抓取 (戴上一般使用者面具防阻擋)
# ==========================================
def fetch_api_data(url, desc):
    """通用官方 API 抓取函數，戴上面具與設定超時"""
    print(f"啟動機器人：鎖定抓取最新 {desc} 資料...")
    
    # 幫機器人戴上一般使用者的面具，避免被政府網站擋下
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        df = pd.DataFrame(data)
        if not df.empty and '公司代號' in df.columns:
            # 統一將關鍵欄位名稱清理成程式好處理的格式
            df = df.rename(columns={'公司代號': '代號'})
            return df
        print(f"[{desc}] 抓取結果為空白或格式不符。")
        return pd.DataFrame()
    except Exception as e:
        print(f"[{desc}] 抓取失敗: {e}")
        return pd.DataFrame()

# ==========================================
# 3. 核心運算：縫合資料與計算配息率
# ==========================================
def calculate_payout_data():
    """分別抓取 EPS 和股利，縫合後算出配息率"""
    
    # 網址 A: 最新累季每股盈餘(元) (TWSE 官方 API)
    url_eps = "https://openapi.twse.com.tw/v1/opendata/t187ap14_L"
    
    # 網址 B: 最新現金股利資料 (TWSE 官方 API)
    url_div = "https://openapi.twse.com.tw/v1/opendata/t187ap11_L"
    
    # 分別抓取
    df_eps_raw = fetch_api_data(url_eps, "EPS")
    df_div_raw = fetch_api_data(url_div, "現金股利")
    
    if df_eps_raw.empty or df_div_raw.empty:
        print("未抓到完整 EPS 或股利資料，結束程式以防資料殘缺。")
        return pd.DataFrame()

    # 清理資料：只留下需要的欄位並重新命名
    # EPS 處理 (假設官方欄位名稱為 基本每股盈餘)
    if '基本每股盈餘' in df_eps_raw.columns:
        df_eps = df_eps_raw[['代號', '基本每股盈餘']].rename(columns={'基本每股盈餘': '最新累季每股盈餘(元)'})
    else:
        df_eps = pd.DataFrame()

    # 股利處理 (假設官方欄位名稱為 現金股利總計 或 股利合計)
    if '現金股利總計' in df_div_raw.columns:
        df_div = df_div_raw[['代號', '現金股利總計']].rename(columns={'現金股利總計': '合計股利'})
    elif '股利合計' in df_div_raw.columns:
        df_div = df_div_raw[['代號', '股利合計']].rename(columns={'股利合計': '合計股利'})
    else:
        df_div = pd.DataFrame()

    if df_eps.empty or df_div.empty:
        print("資料欄位名稱解析錯誤，結束程式。")
        return pd.DataFrame()

    # 🚀 將 EPS 和股利資料缝合在一起 (Left Join)
    df_merged = pd.merge(df_eps, df_div, on='代號', how='left')
    
    # 確保數值欄位為數字格式
    df_merged['最新累季每股盈餘(元)'] = pd.to_numeric(df_merged['最新累季每股盈餘(元)'], errors='coerce')
    df_merged['合計股利'] = pd.to_numeric(df_merged['合計股利'], errors='coerce')
    
    # 🧠 自動算出並填入「盈餘總分配率」 (股利 / EPS * 100)
    # 使用 pd.notna(df_merged['合計股利']) 來確保有抓到股利才算，否則保留為空
    def calc_ratio(row):
        eps = row['最新累季每股盈餘(元)']
        div = row['合計股利']
        
        # 只有當 EPS 為正，且股利資料不為空時，才算出配息率
        if pd.notna(div) and pd.notna(eps) and eps > 0:
            return (div / eps) * 100
        else:
            return None # 虧損或沒抓到股利時，配息率保留為空

    df_merged['盈餘總分配率'] = df_merged.apply(calc_ratio, axis=1)
    
    return df_merged[['代號', '最新累季每股盈餘(元)', '合計股利', '盈餘總分配率']]

# ==========================================
# 4. 更新至 Google Sheet 當年度表
# ==========================================
def update_google_sheet(df_calculated_data):
    if df_calculated_data.empty:
        print("沒有新的計算資料需要更新。")
        return
        
    client = get_gspread_client()
    spreadsheet = client.open_by_url(MASTER_GSHEET_URL)
    
    # 尋找「當年度表」分頁
    target_worksheets = [ws for ws in spreadsheet.worksheets() if "當年度表" in ws.title]
    
    if not target_worksheets:
        print("找不到名稱包含「當年度表」的分頁。")
        return

    for ws in target_worksheets:
        print(f"正在更新表單: {ws.title}")
        data = ws.get_all_values()
        if len(data) > 1:
            df_sheet = pd.DataFrame(data[1:], columns=data[0])
            
            # 定義需要更新的目標欄位清單
            target_cols = ['最新累季每股盈餘(元)', '合計股利', '盈餘總分配率']
            
            # 取得各目標欄位在 Google Sheet 裡面的索引 (從 1 開始)
            col_indices = {}
            for col_name in target_cols:
                if col_name in df_sheet.columns:
                    col_indices[col_name] = df_sheet.columns.get_loc(col_name) + 1
            
            # 將計算好的資料一一更新進表單
            for index, row in df_calculated_data.iterrows():
                stock_id = str(row['代號'])
                
                # 找到對應的股票代號列
                if stock_id in df_sheet['代號'].values:
                    # Google Sheet 列數 = pandas 索引 + 2 (因為 pandas 從0開始，Google Sheet 從1開始且有標題列)
                    row_idx = df_sheet.index[df_sheet['代號'] == stock_id].tolist()[0] + 2
                    
                    for col_name, col_idx in col_indices.items():
                        # 處理 None 或 NaN 值為空字串
                        value_to_update = row[col_name]
                        if pd.isna(value_to_update):
                            value_to_update = "" # 清空該格子
                        else:
                            value_to_update = str(value_to_update)
                            
                        ws.update_cell(row_idx, col_idx, value_to_update)
                        time.sleep(0.5) # 稍微放慢更新速度，避免被 Google API 擋下

            print(f"{ws.title} 更新完成！")

if __name__ == "__main__":
    calculated_data = calculate_payout_data()
    update_google_sheet(calculated_data)
    print("全自動財報/股利/配息率更新排程執行完畢！")
