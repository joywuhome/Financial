import pandas as pd
import streamlit as st

# ==========================================
# 🌟 升級版：核心快取大腦 (自動縫合當年度與歷史表單)
# ==========================================
@st.cache_data(ttl=3600, show_spinner="連線至大數據庫並進行資料縫合...")
def fetch_gsheet_data_v200():
    try:
        client = get_gspread_client()
        spreadsheet = client.open_by_url(MASTER_GSHEET_URL)
        worksheets = spreadsheet.worksheets()
        
        df_current_list = []
        df_history_list = []
        
        # 1. 自動把 7 張表分類打包
        for ws in worksheets:
            if "當年度表" in ws.title:
                data = ws.get_all_values()
                if data and len(data) > 1:
                    df = pd.DataFrame(data[1:], columns=data[0])
                    df_current_list.append(df)
            elif "歷史表單" in ws.title:
                data = ws.get_all_values()
                if data and len(data) > 1:
                    df = pd.DataFrame(data[1:], columns=data[0])
                    df_history_list.append(df)
                    
        # 將所有同類型的表單垂直合併
        df_current = pd.concat(df_current_list, ignore_index=True) if df_current_list else pd.DataFrame()
        df_history = pd.concat(df_history_list, ignore_index=True) if df_history_list else pd.DataFrame()

        # 2. 🚀 記憶體縫合術：用「代號」把歷史資料接在當年度資料後面
        if not df_current.empty and not df_history.empty:
            if "代號" in df_current.columns and "代號" in df_history.columns:
                # 為了避免「名稱」重複出現，先把歷史表的名稱拿掉
                if "名稱" in df_history.columns:
                    df_history = df_history.drop(columns=["名稱"])
                
                # 執行縫合 (Left Join)
                df_combined = pd.merge(df_current, df_history, on="代號", how="left")
            else:
                df_combined = df_current
        else:
            df_combined = df_current

        # 3. 把縫合好的超級大表，交給您原本寫好的解析器
        # (這裡完美銜接您原本的配息防守、本益比推算等戰略邏輯)
        def parse_df(df):
            if df is None or df.empty: return {}
            # 💡 您原本在 parse_df 裡面的所有運算邏輯 (抓 YoY、扣業外等) 都不用改，直接放在這裡！
            # ... [保留您 V182 版的戰略邏輯] ...
            
        return {"general": parse_df(df_combined), "finance": {}}
    except Exception as e: 
        return {"error": str(e)}
