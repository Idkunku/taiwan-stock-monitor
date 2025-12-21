# -*- coding: utf-8 -*-
import os, time, random, json, subprocess
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 參數與路徑 ==========
MARKET_CODE = "cn-share"
DATA_SUBDIR = "dayK"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, DATA_SUBDIR)
# 快取路徑：放在 data/cn-share/lists 下以保持結構整潔
LIST_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, "lists")
CACHE_LIST_PATH = os.path.join(LIST_DIR, "cn_stock_list_cache.json")

THREADS_CN = 4
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LIST_DIR, exist_ok=True)

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

def get_cn_list():
    """使用 akshare 獲取 A 股清單，具備多重防呆與重試機制"""
    threshold = 4000  # 正常 A 股應有 5000 檔以上，低於 4000 視為異常
    
    # 1. 檢查今日快取
    if os.path.exists(CACHE_LIST_PATH):
        try:
            file_mtime = os.path.getmtime(CACHE_LIST_PATH)
            if datetime.fromtimestamp(file_mtime).date() == datetime.now().date():
                with open(CACHE_LIST_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if len(data) >= threshold:
                        log(f"📦 載入今日 A 股清單快取 (共 {len(data)} 檔)...")
                        return data
        except Exception as e:
            log(f"⚠️ 讀取快取失敗: {e}")

    # 2. 進入 API 重試迴圈
    import akshare as ak
    max_retries = 3
    for i in range(max_retries):
        log(f"📡 正在從 akshare 獲取清單 (第 {i+1}/{max_retries} 次嘗試)...")
        try:
            # 獲取 A 股代碼名稱
            df = ak.stock_info_a_code_name()
            
            # 代碼補齊與過濾 (確保 000001 這種格式)
            df['code'] = df['code'].astype(str).str.zfill(6)
            valid_prefixes = ('000','001','002','003','300','301','302','600','601','603','605','688','689')
            df = df[df['code'].str.startswith(valid_prefixes)]
            
            res = [f"{row['code']}&{row['name']}" for _, row in df.iterrows()]
            
            # 數量門檻判定
            if len(res) >= threshold:
                log(f"✅ 成功獲取 A 股清單，共 {len(res)} 檔。")
                with open(CACHE_LIST_PATH, "w", encoding="utf-8") as f:
                    json.dump(res, f, ensure_ascii=False)
                return res
            else:
                log(f"⚠️ 數量異常 ({len(res)} 檔)，準備重試...")
                
        except Exception as e:
            log(f"❌ 嘗試失敗: {e}")
        
        if i < max_retries - 1:
            wait_time = (i + 1) * 5
            log(f"⏳ 等待 {wait_time} 秒後重試...")
            time.sleep(wait_time)

    # 3. 終極保底：讀取歷史快取
    if os.path.exists(CACHE_LIST_PATH):
        log("🔄 API 請求全數失敗，使用歷史快取備援...")
        with open(CACHE_LIST_PATH, "r", encoding="utf-8") as f:
            return json.load(f)

    log("🚨 警告：完全無法取得清單，執行最小測試集。")
    return ["600519&貴州茅台", "000001&平安銀行"]

def download_one(item):
    """單檔下載邏輯"""
    try:
        code, name = item.split('&', 1)
        # Yahoo Finance 格式：6 開頭為上海 (.SS)，其餘為深圳 (.SZ)
        symbol = f"{code}.SS" if code.startswith('6') else f"{code}.SZ"
        # 檔名格式：Code_Name.csv
        out_path = os.path.join(DATA_DIR, f"{code}_{name}.csv")

        # 檢查檔案是否已存在且有內容 (續跑機制)
        if os.path.exists(out_path) and os.path.getsize(out_path) > 1000:
            return {"status": "exists", "code": code}

        # 適度暫停避免被限流
        time.sleep(random.uniform(0.3, 0.8))
        
        tk = yf.Ticker(symbol)
        # 抓取 2 年日 K
        hist = tk.history(period="2y", timeout=20)
        
        if hist is not None and not hist.empty:
            hist.reset_index(inplace=True)
            hist.columns = [c.lower() for c in hist.columns]
            # 確保日期沒有時區問題
            if 'date' in hist.columns:
                hist['date'] = pd.to_datetime(hist['date'], utc=True).dt.tz_localize(None)
            
            hist.to_csv(out_path, index=False, encoding='utf-8-sig')
            return {"status": "success", "code": code}
        return {"status": "empty", "code": code}
    except Exception:
        return {"status": "error", "code": item.split('&')[0]}

def main():
    log("🇨🇳 中國 A 股 K 線下載器啟動")
    items = get_cn_list()
    log(f"🚀 開始下載處理 (共 {len(items)} 檔標的)")
    
    stats = {"success": 0, "exists": 0, "empty": 0, "error": 0}
    
    # 使用多執行緒提升下載速度
    with ThreadPoolExecutor(max_workers=THREADS_CN) as executor:
        futs = {executor.submit(download_one, it): it for it in items}
        pbar = tqdm(total=len(items), desc="CN 下載進度")
        
        for f in as_completed(futs):
            res = f.result()
            stats[res.get("status", "error")] += 1
            pbar.update(1)
        
        pbar.close()
    
    log(f"📊 A 股下載報告: 成功={stats['success']}, 跳過(已存在)={stats['exists']}, 失敗={stats['error']}")

if __name__ == "__main__":
    main()
