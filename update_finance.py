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
# 這是您的 Google Sheet 總表網址 (請確認網址正確)
MASTER_GSHEET_URL = "https://docs.google.com/spreadsheets/d/1Z_u8r0pB2K90t3pG5m5m-E02n_H_hG5s4-Y4Qo-tZgI/edit"

def get_gspread_client():
    """讀取 GitHub 保險箱裡的金鑰"""
    key_data = os.environ.get("GOOGLE_CREDENTIALS")
    if not key_data:
        raise ValueError("找不到 Google 金鑰環境變數，請檢查 GitHub Secrets 設定！")
    
    creds_dict = json.loads(key_data)
    creds = Credentials.from_service_account_info(creds_dict, scopes=['https://www.googleapis.com/auth/spreadsheets'])
    return gspread.authorize(creds)

# ==========================================
# 2. 官方資料抓取 (戴上瀏覽器面具防阻擋)
# ==========================================
def fetch_latest_eps():
    """前往政府公開資訊觀測站抓取最新 EPS"""
    print("啟動財報更新機器人：鎖定抓取最新 EPS 資料...")
    url = "https://openapi.twse.com.tw/v1/opendata/t187ap14_L"
    
    # 幫機器人戴上一般使用者的面具
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        df = pd.DataFrame(data)
        # 假設官方欄位名稱為 公司代號, 基本每股盈餘
        if not df.empty and '公司代號' in df.columns:
            df = df.rename(columns={'公司代號': '代號', '基本每股盈餘': '最新累季每股盈餘(元)'})
            return df[['代號', '最新累季每股盈餘(元)']]
        return pd.DataFrame()
    except Exception as e:
        print(f"抓取失敗: {e}")
        return pd.DataFrame()

# ==========================================
# 3. 更新至 Google Sheet 當年度表
# ==========================================
def update_google_sheet(df_new_data):
    if df_new_data.empty:
        print("沒有抓到新資料，結束程式。")
        return
        
    client = get_gspread_client()
    spreadsheet = client.open_by_url(MASTER_GSHEET_URL)
    
    # 尋找「當年度表01」並更新
    for ws in spreadsheet.worksheets():
        if "當年度表" in ws.title:
            print(f"正在更新表單: {ws.title}")
            data = ws.get_all_values()
            if len(data) > 1:
                df_sheet = pd.DataFrame(data[1:], columns=data[0])
                
                # 將新抓到的 EPS 更新進表單
                for index, row in df_new_data.iterrows():
                    stock_id = str(row['代號'])
                    new_eps = str(row['最新累季每股盈餘(元)'])
                    
                    # 找到對應的股票代號列
                    if stock_id in df_sheet['代號'].values:
                        row_idx = df_sheet.index[df_sheet['代號'] == stock_id].tolist()[0]
                        # 找到 EPS 欄位的位置並更新 (Google Sheet 索引從 1 開始)
                        if '最新累季每股盈餘(元)' in df_sheet.columns:
                            col_idx = df_sheet.columns.get_loc('最新累季每股盈餘(元)') + 1
                            ws.update_cell(row_idx + 2, col_idx, new_eps)
                            time.sleep(1) # 避免更新太快被 Google 擋下
            print(f"{ws.title} 更新完成！")

if __name__ == "__main__":
    new_data = fetch_latest_eps()
    update_google_sheet(new_data)
    print("全自動更新排程執行完畢！")
