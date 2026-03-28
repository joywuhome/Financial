# ==========================================
# 📂 檔案名稱： Financial_API.py (黃金公式絕對鎖死版)
# 💡 更新內容： 嚴格執行 1月x2+2月 營收邏輯、Q4/Q3 業外自動判讀，以及新增 Q2 (4、5月) 動態推估公式！
# ==========================================

import streamlit as st
import pandas as pd
import io
import altair as alt
import re
import os
import requests
import gspread
from google.oauth2.service_account import Credentials
import json
import urllib3
import time
import math
import numpy as np
import yfinance as yf
from datetime import datetime

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================
# 網頁基本設定 & 響應式 CSS 
# ==========================================
st.set_page_config(page_title="2026 戰略指揮 (精準校準版)", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
    <style>
    h1 { font-size: 1.8rem !important; margin-bottom: 0px !important; }
    h2 { font-size: 1.4rem !important; margin-bottom: 0px !important; }
    h3 { font-size: 1.2rem !important; margin-bottom: 0.5rem !important; } 
    p { margin-bottom: 0.2rem !important; font-size: 0.95rem !important; }
    .block-container { padding-top: 2.5rem !important; padding-bottom: 1rem !important; }
    @media (max-width: 768px) {
        h1 { font-size: 1.5rem !important; }
        h2 { font-size: 1.2rem !important; }
        h3 { font-size: 1.05rem !important; margin-bottom: 0.2rem !important; } 
        .block-container { padding-top: 1.5rem !important; }
    }
    ::-webkit-scrollbar { width: 14px !important; height: 14px !important; }
    ::-webkit-scrollbar-track { background: #e0e0e0; border-radius: 6px; }
    ::-webkit-scrollbar-thumb { background: #888; border-radius: 6px; border: 2px solid #e0e0e0; }
    ::-webkit-scrollbar-thumb:hover { background: #555; }
    div[data-testid="stDataFrame"] div { scrollbar-width: auto; }
    </style>
""", unsafe_allow_html=True)

MASTER_GSHEET_URL = "https://docs.google.com/spreadsheets/d/1vsqhH2i8aoRnBwPJ4BJ1eL2vQYGCkqabgG08f8P2A2c/edit"

def force_rerun():
    try:
        st.rerun()
    except AttributeError:
        st.experimental_rerun()

def clear_cache_and_session():
    st.cache_data.clear()
    for key in list(st.session_state.keys()):
        del st.session_state[key]

def get_gspread_client():
    if "GOOGLE_CREDENTIALS" not in st.secrets: raise ValueError("找不到 Google 金鑰")
    key_data = st.secrets["GOOGLE_CREDENTIALS"]
    creds = Credentials.from_service_account_info(json.loads(key_data) if isinstance(key_data, str) else dict(key_data), scopes=['https://www.googleapis.com/auth/spreadsheets'])
    return gspread.authorize(creds)

def get_realtime_price(code, default_price):
    try:
        p = yf.Ticker(f"{code}.TW").fast_info['last_price']
        if p > 0 and not math.isnan(p): return float(p)
    except: pass
    try:
        p = yf.Ticker(f"{code}.TWO").fast_info['last_price']
        if p > 0 and not math.isnan(p): return float(p)
    except: pass
    
    headers = {'User-Agent': 'Mozilla/5.0'}
    for sfx in ['.TW', '.TWO']:
        try:
            res = requests.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{code}{sfx}", headers=headers, timeout=2, verify=False).json()
            p = res['chart']['result'][0]['meta']['regularMarketPrice']
            if p > 0 and not math.isnan(p): return float(p)
        except: pass
    return default_price

st.title("📊 2026 戰略指揮 (精準校準版)")

# ==========================================
# 📊 核心大腦一：一般/成長股預估引擎
# ==========================================
def auto_strategic_model(name, current_month, rev_last_10, rev_last_11, rev_last_12, rev_this_1, rev_this_2, rev_this_3, rev_this_4, rev_this_5, base_q_eps, non_op_ratio, base_q_total_rev, ly_q1_rev, ly_q2_rev, ly_q3_rev, ly_q4_rev, y1_q1_rev, y1_q2_rev, y1_q3_rev, y1_q4_rev, recent_payout_ratio, current_price, contract_liab, contract_liab_qoq, acc_eps, declared_div, actual_q1_eps):
    try:
        current_price = float(current_price)
        if math.isnan(current_price) or math.isinf(current_price): current_price = 0.0
    except: current_price = 0.0

    if current_month <= 1: sim_rev_1, sim_rev_2, sim_rev_3 = 0, 0, 0
    elif current_month == 2: sim_rev_1, sim_rev_2, sim_rev_3 = rev_this_1, 0, 0
    elif current_month == 3: sim_rev_1, sim_rev_2, sim_rev_3 = rev_this_1, rev_this_2, 0
    else: sim_rev_1, sim_rev_2, sim_rev_3 = rev_this_1, rev_this_2, rev_this_3

    actual_known_q1 = sum([v for v in [sim_rev_1, sim_rev_2, sim_rev_3] if v > 0])
    
    ratio_q1 = ly_q1_rev / y1_q4_rev if y1_q4_rev > 0 else 1.0
    sum_q2_history = y1_q2_rev + ly_q2_rev
    sum_q3_history = y1_q3_rev + ly_q3_rev
    sum_q4_history = y1_q4_rev + ly_q4_rev
    
    ratio_q3 = sum_q3_history / sum_q2_history if sum_q2_history > 0 else 1.0
    ratio_q4 = sum_q4_history / sum_q3_history if sum_q3_history > 0 else 1.0

    # 🌟 紅色標竿
    if rev_last_12 > 0:
        base_11_12_avg = (rev_last_11 + rev_last_12) / 2
    else:
        base_11_12_avg = (rev_last_10 + rev_last_11 + (rev_last_11 * 0.9)) / 3
        
    benchmark_q1_rev = (base_11_12_avg * 3) * ratio_q1 
    benchmark_q2_rev = benchmark_q1_rev
    benchmark_q3_rev = benchmark_q2_rev * ratio_q3
    benchmark_q4_rev = benchmark_q3_rev * ratio_q4

    # 🌟 嚴格遵守指定邏輯：(1月x2 + 2月)
    if current_month <= 1: 
        dynamic_est_q1_rev = benchmark_q1_rev
        dynamic_base_avg = base_11_12_avg
        formula_note = "動態EPS推估 (全未知)"
    elif current_month == 2: 
        if sim_rev_1 > 0:
            dynamic_est_q1_rev = sim_rev_1 * 0.9 * 3
            dynamic_base_avg = dynamic_est_q1_rev / 3
            formula_note = "動態EPS推估 (1月x0.9x3)"
        else:
            dynamic_est_q1_rev = benchmark_q1_rev
            dynamic_base_avg = base_11_12_avg
            formula_note = "動態EPS推估 (無1月用標竿)"
    elif current_month == 3: 
        if sim_rev_2 > 0:
            # 💡 就是這裡！乖乖換回 1月x2 + 2月
            dynamic_est_q1_rev = (sim_rev_1 * 2) + sim_rev_2
            dynamic_base_avg = dynamic_est_q1_rev / 3
            formula_note = "動態EPS推估 (1月x2+2月)"
        elif sim_rev_1 > 0:
            dynamic_est_q1_rev = sim_rev_1 * 0.9 * 3
            dynamic_base_avg = dynamic_est_q1_rev / 3
            formula_note = "動態EPS推估 (僅知1月x0.9x3)"
        else:
            dynamic_est_q1_rev = benchmark_q1_rev
            dynamic_base_avg = base_11_12_avg
            formula_note = "動態EPS推估 (無1,2月用標竿)"
    else: 
        dynamic_est_q1_rev = sim_rev_1 + sim_rev_2 + sim_rev_3
        dynamic_base_avg = dynamic_est_q1_rev / 3
        formula_note = "動態EPS推估 (知Q1)"

    # ==========================================
    # 🌟 專屬 Q2 動態營收推估 (4/5 月黃金公式)
    # ==========================================
    if current_month <= 3:
        # 1~3月：因無 Q2 數據，直接平移 Q1 預估值，避免混淆
        dynamic_est_q2_rev = dynamic_est_q1_rev
    elif current_month == 4:
        # 4月：已知4月。公式 = 4月x2 + (4月x0.9)
        if rev_this_4 > 0:
            dynamic_est_q2_rev = (rev_this_4 * 2) + (rev_this_4 * 0.9)
            formula_note += " (Q2:4月x2+0.9)"
        else:
            dynamic_est_q2_rev = dynamic_est_q1_rev
    else: 
        # 5月(含)以上：已知4、5月。公式 = 4月 + 5月 + (5月x0.9)
        if rev_this_4 > 0 and rev_this_5 > 0:
            dynamic_est_q2_rev = rev_this_4 + rev_this_5 + (rev_this_5 * 0.9)
            formula_note += " (Q2:4+5+5月x0.9)"
        elif rev_this_4 > 0:
            dynamic_est_q2_rev = (rev_this_4 * 2) + (rev_this_4 * 0.9) # 防守退守
            formula_note += " (Q2:缺5月退守4月公式)"
        else:
            dynamic_est_q2_rev = dynamic_est_q1_rev

    dynamic_est_q3_rev = dynamic_est_q2_rev * ratio_q3
    dynamic_est_q4_rev = dynamic_est_q3_rev * ratio_q4
    dynamic_total_rev = dynamic_est_q1_rev + dynamic_est_q2_rev + dynamic_est_q3_rev + dynamic_est_q4_rev

    # 🌟 EPS 黃金公式
    safe_base_rev = base_q_total_rev if base_q_total_rev > 0 else 1.0
    profit_margin_factor = base_q_eps * (1 - (non_op_ratio / 100)) / safe_base_rev 

    est_q1_eps_forecast = dynamic_est_q1_rev * profit_margin_factor
    est_q2_eps_forecast = dynamic_est_q2_rev * profit_margin_factor
    est_q3_eps_forecast = dynamic_est_q3_rev * profit_margin_factor
    est_q4_eps_forecast = dynamic_est_q4_rev * profit_margin_factor

    if actual_q1_eps >
