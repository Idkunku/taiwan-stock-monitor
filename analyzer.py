# -*- coding: utf-8 -*-
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from tqdm import tqdm
import matplotlib

# 強制使用 Agg 後端以在雲端伺服器運行
matplotlib.use('Agg')

# 設定字型：解決 GitHub Actions 中文亂碼問題
plt.rcParams['font.sans-serif'] = [
    'Noto Sans CJK TC', 
    'Noto Sans CJK JP', 
    'Microsoft JhengHei', 
    'Arial Unicode MS', 
    'sans-serif'
]
plt.rcParams['axes.unicode_minus'] = False

# 分箱參數
BIN_SIZE = 10.0
X_MIN, X_MAX = -100, 100
BINS = np.arange(X_MIN, X_MAX + 11, BIN_SIZE)

def build_company_list(arr_pct, codes, names, bins):
    """產出 HTML 格式的分箱清單，連結導向技術圖表"""
    lines = [f"{'報酬區間':<12} | {'家數(比例)':<14} | 公司清單", "-"*80]
    total = len(arr_pct)
    clipped_arr = np.clip(arr_pct, -100, 100)
    counts, edges = np.histogram(clipped_arr, bins=bins)

    for i in range(len(edges)-1):
        lo, up = edges[i], edges[i+1]
        lab = f"{int(lo)}%~{int(up)}%"
        mask = (arr_pct >= lo) & (arr_pct < up)
        if i == len(edges) - 2: mask = (arr_pct >= lo) & (arr_pct <= up)

        cnt = int(mask.sum())
        if cnt == 0: continue

        picked_indices = np.where(mask)[0]
        links = []
        for idx in picked_indices:
            code, name = codes[idx], names[idx]
            # ✅ 修正：連結改為 technical-chart
            link = f'<a href="https://www.wantgoo.com/stock/{code}/technical-chart" style="text-decoration:none; color:#0366d6;">{code}({name})</a>'
            links.append(link)
        
        lines.append(f"{lab:<12} | {cnt:>4} ({(cnt/total*100):5.1f}%) | {', '.join(links)}")
    return "\n".join(lines)

def run_global_analysis(market_id="tw-share"):
    print(f"📊 正在啟動 {market_id} 深度矩陣分析...")
    data_path = Path("./data") / market_id / "dayK"
    image_out_dir = Path("./output/images") / market_id
    image_out_dir.mkdir(parents=True, exist_ok=True)
    
    all_files = list(data_path.glob("*.csv"))
    if not all_files: return [], pd.DataFrame(), {}

    results = []
    for f in tqdm(all_files, desc="分析數據"):
        try:
            df = pd.read_csv(f)
            if len(df) < 20: continue
            df.columns = [c.lower() for c in df.columns]
            close, high, low = df['close'].values, df['high'].values, df['low'].values
            periods = [('Week', 5), ('Month', 20), ('Year', 250)]
            filename = f.stem
            tkr, nm = filename.split('_', 1) if '_' in filename else (filename, filename)
            row = {'Ticker': tkr, 'Full_ID': nm}
            for p_name, days in periods:
                if len(close) <= days: continue
                prev_c = close[-(days+1)]
                if prev_c == 0: continue
                row[f'{p_name}_High'] = (max(high[-days:]) - prev_c) / prev_c * 100
                row[f'{p_name}_Close'] = (close[-1] - prev_c) / prev_c * 100
                row[f'{p_name}_Low'] = (min(low[-days:]) - prev_c) / prev_c * 100
            results.append(row)
        except: continue

    df_res = pd.DataFrame(results)
    images = []
    for p_n, p_z in [('Week', '週'), ('Month', '月'), ('Year', '年')]:
        for t_n, t_z in [('High', '最高-進攻'), ('Close', '收盤-實質'), ('Low', '最低-防禦')]:
            col = f"{p_n}_{t_n}"
            if col not in df_res.columns: continue
            data = df_res[col].dropna()
            fig, ax = plt.subplots(figsize=(11, 7))
            counts, edges = np.histogram(np.clip(data.values, X_MIN, X_MAX), bins=BINS)
            bars = ax.bar(edges[:-1], counts, width=9, align='edge', color='#007bff', alpha=0.7)
            ax.set_title(f"{p_z}K {t_z} 報酬分布", fontsize=18, fontweight='bold')
            img_path = image_out_dir / f"{col.lower()}.png"
            plt.savefig(img_path, dpi=120)
            plt.close()
            images.append({'id': col.lower(), 'path': str(img_path), 'label': f"{p_z}K {t_z}"})

    text_reports = {}
    for p_n, p_z in [('Week', '週K'), ('Month', '月K'), ('Year', '年K')]:
        col = f'{p_n}_High'
        if col in df_res.columns:
            text_reports[p_n] = build_company_list(df_res[col].values, df_res['Ticker'].tolist(), df_res['Full_ID'].tolist(), BINS)
    
    return images, df_res, text_reports
