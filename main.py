import io
import requests
import pandas as pd
import time
import os
import shutil
import random
import warnings
import json
from datetime import datetime
from openpyxl import Workbook, load_workbook
from openpyxl.styles import PatternFill

# Google API 相關庫
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

warnings.filterwarnings("ignore", message="Unverified HTTPS request")

# === 1. 權限與路徑設定 ===
# 這裡會從 GitHub Secrets 讀取你存入的 GCP_SERVICE_ACCOUNT_KEY
SERVICE_ACCOUNT_INFO = json.loads(os.environ.get('GCP_SERVICE_ACCOUNT_KEY'))
SCOPES = ['https://www.googleapis.com/auth/drive']
creds = service_account.Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO, scopes=SCOPES)
drive_service = build('drive', 'v3', credentials=creds)

# 【請務必修改這裡的 ID】
# 請進入雲端資料夾後，從網址列最後一段取得那一串亂碼
BASE_DIR_ID = "1WE_tmOMxh9zffOVau4lAFOTnWDWFy_wJ"
BACKUP_DIR_ID = "11YNuJdVqUkAJTDhN4ASIkrxGDK3_PDUO"

# GitHub Actions 虛擬機內的臨時資料夾
LOCAL_TEMP = "./temp_stock"
if not os.path.exists(LOCAL_TEMP):
    os.makedirs(LOCAL_TEMP)

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
})

# === 2. 備份功能 (雲端 API 複製版) ===
def perform_backup():
    today_str = datetime.now().strftime("%Y%m%d")
    print(f"📦 準備備份雲端資料至 {today_str}...")

    # 建立日期子資料夾
    folder_metadata = {
        'name': today_str,
        'mimeType': 'application/vnd.google-apps.folder',
        'parents': [BACKUP_DIR_ID]
    }
    folder = drive_service.files().create(body=folder_metadata, fields='id').execute()
    new_folder_id = folder.get('id')

    # 搜尋並複製所有價量報表
    query = f"'{BASE_DIR_ID}' in parents and name contains '_價量報表.xlsx' and trashed = false"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])

    for f in files:
        drive_service.files().copy(
            fileId=f['id'],
            body={'parents': [new_folder_id]},
            supportsAllDrives=True  # 確保支援共用硬碟格式
        ).execute()
    print(f"✅ 雲端備份完成！共複製 {len(files)} 個檔案。")

# === 3. 股票抓取功能 ===
def get_all_taiwan_stocks():
    def get_stocks(mode):
        url = f"https://isin.twse.com.tw/isin/C_public.jsp?strMode={mode}"
        res = session.get(url, verify=False)
        res.encoding = "big5"
        df = pd.read_html(io.StringIO(res.text))[0]
        df.columns = ["有價證券代號及名稱", "ISIN", "日期", "市場別", "產業別", "CFICode", "備註"]
        df = df[df["CFICode"] == "ESVUFR"]
        return {row.split()[0]: row.split()[1] for row in df["有價證券代號及名稱"] if len(row.split()) >= 2}
    
    all_s = {**get_stocks(2), **get_stocks(4)}
    print(f"📚 合計股票: {len(all_s)} 檔")
    return all_s

def fetch_price_by_volume(symbol="2330.TW"):
    """取得 Yahoo 股價價量分佈資料（自動容錯解析）"""
    url = f"https://tw.stock.yahoo.com/_td-stock/api/resource/StockServices.priceByVolumes;symbol={symbol}?bkt=%5B%22tw-stock-desktop-future-rampup%22%5D&device=desktop&lang=zh-Hant-TW&region=TW"
    headers = {"User-Agent": "Mozilla/5.0"}
    res = session.get(url, headers=headers)
    res.raise_for_status()
    js = res.json()

    # --------------------------
    #  1️⃣ 找 priceByVolumes
    # --------------------------
    data = None
    for path in [
        ["data", "priceByVolumes"],
        ["priceByVolumes"],
        ["data", "data", "priceByVolumes"],
    ]:
        d = js
        try:
            for k in path:
                d = d[k]
            if isinstance(d, list):
                data = d
                break
        except (KeyError, TypeError):
            continue

    if not data:
        print("⚠️ 無法從 Yahoo 回傳中找到 priceByVolumes，實際回傳內容：")
        print(js)
        raise ValueError("Yahoo API 結構已更改，請檢查回傳格式。")

    price_list = [(float(d["price"]), int(d["volumeK"])) for d in data if d.get("volumeK")]

    # --------------------------
    #  2️⃣ 找 日期（多層容錯）
    # --------------------------
    date = None
    date_paths = [
        ["data", "date"],
        ["date"],
        ["data", "data", "date"],
        ["meta", "date"],
    ]

    for path in date_paths:
        d = js
        try:
            for k in path:
                d = d[k]
            if isinstance(d, str) and len(d) >= 10:
                date = d[:10]
                break
        except Exception:
            continue

    # 若仍然找不到，用今日系統日期（最後保底方案）
    if date is None:
        from datetime import datetime
        date = datetime.now().strftime("%Y-%m-%d")

    return {
        "date": date,
        "data": price_list,
    }


def fetch_margin_data(symbol="2330.TW"):
    """取得 Yahoo 融資融券資料（自動容錯解析）"""
    url = f"https://tw.stock.yahoo.com/_td-stock/api/resource/StockServices.creditsWithQuoteStats;limit=90;symbol={symbol}?bkt=%5B%22tw-stock-desktop-future-rampup%22%5D&device=desktop&lang=zh-Hant-TW&region=TW"
    headers = {"User-Agent": "Mozilla/5.0"}
    res = session.get(url, headers=headers)
    res.raise_for_status()
    js = res.json()

    # --- 多層防呆找資料 ---
    possible_paths = [
        ["data", "data", "result", "credits"],
        ["data", "result", "credits"],
        ["result", "credits"],
        ["credits"],
    ]

    credits = None
    for path in possible_paths:
        d = js
        try:
            for k in path:
                d = d[k]
            if isinstance(d, list) and len(d) > 0:
                credits = d[0]
                break
        except (KeyError, TypeError):
            continue

    if not credits:
        print("⚠️ 無法從 Yahoo 回傳中找到 credits 資料，實際回傳內容：")
        print(js)
        raise ValueError("Yahoo API 結構已更改，請檢查回傳格式。")

    # --- 萃取日期、融資、融券 ---
    date = credits.get("date", "").split("T")[0]
    fin = int(credits.get("financingTotalVolK", 0))
    short = int(credits.get("shortTotalVolK", 0))

    return {
        "date": date,
        "融資": fin,
        "融券": short,
    }

def safe_fetch_margin_data(symbol, prev_margin_data=None):
    """
    嘗試安全抓取 margin data；若失敗，回傳預設值。
    prev_margin_data: 昨日資料（若要計算融資差與融券差）
    """
    try:
        margin = fetch_margin_data(symbol)

        # 若有前一日資料，計算變化量
        if prev_margin_data:
            margin["融資差"] = margin["融資"] - prev_margin_data.get("融資", 0)
            margin["融券差"] = margin["融券"] - prev_margin_data.get("融券", 0)
        else:
            margin["融資差"] = 0
            margin["融券差"] = 0

        return margin

    except Exception as e:
        print(f"⚠️ [警告] 無法取得 {symbol} 的資券資料: {e}")
        # 若發生錯誤，回傳預設為 0 的結構
        return {
            "date": pd.Timestamp.now().strftime("%Y-%m-%d"),
            "融資": 0,
            "融資差": 0,
            "融券": 0,
            "融券差": 0,
        }

def _safe_raw(x):
    """將 {'raw': '21.5'} 或 '21.5' 或 None -> float 或 None"""
    try:
        if x is None:
            return None
        if isinstance(x, dict) and "raw" in x:
            return float(x["raw"])
        return float(x)
    except:
        return None
        
def fetch_ohlc_data(symbol="2330.TW"):
    """用 path 方式從 Yahoo StockServices.stockList 抓 OHLC"""

    url = (
        "https://tw.stock.yahoo.com/_td-stock/api/resource/StockServices.stockList"
        f";fields=avgPrice%2Corderbook;symbols={symbol}"
        "?device=desktop&ecma=modern&intl=tw&lang=zh-Hant-TW&returnMeta=true"
    )
    headers = {"User-Agent": "Mozilla/5.0"}
    res = requests.get(url, headers=headers, timeout=10)
    res.raise_for_status()
    js = res.json()

    # 可能的結構 path（Yahoo 偶爾會改結構）
    paths = [
        ["data", "list", 0],      # 最常見
        ["data", 0],              # 偶爾 Yahoo 改成這樣
        ["list", 0],              # 另外種可能
        ["data"],                 # 若只有一個物件直接 data
    ]

    item = None
    for path in paths:
        d = js
        try:
            for k in path:
                if isinstance(k, int):
                    d = d[k]
                else:
                    d = d[k]
            # 如果有 price 就當成成功
            if "price" in d:
                item = d
                break
        except Exception:
            continue

    if item is None:
        print("⚠️ Yahoo API 結構變更，找不到 OHLC 資料。回傳內容：")
        print(js)
        raise ValueError("無法從 Yahoo 回傳取得 OHLC 欄位")

    # --- OHLC ---
    pre_close_val = _safe_raw(item.get("regularMarketPreviousClose"))
    high_val  = _safe_raw(item.get("regularMarketDayHigh"))
    low_val  = _safe_raw(item.get("regularMarketDayLow"))
    open_val   = _safe_raw(item.get("regularMarketOpen"))
    close_val = _safe_raw(item.get("price"))

    # --- regularMarketTime 取日期 ---
    time_str = item.get("regularMarketTime")
    if isinstance(time_str, str) and len(time_str) >= 10:
        date = time_str[:10]
    else:
        date = None  # or today's date if needed

    return {
        "date": date,
        "開": open_val,
        "高": high_val,
        "低": low_val,
        "收": close_val,
        "昨收": pre_close_val
    }

# === 4. Excel 處理邏輯 (雲端同步版) ===
def update_excel_cloud(symbol, name, price_data, margin_data, ohlc_data):
    file_name = f"{symbol}_價量報表.xlsx"
    local_path = os.path.join(LOCAL_TEMP, file_name)
    today = margin_data["date"]
    
    # A. 搜尋雲端是否有舊檔
    query = f"name = '{file_name}' and '{BASE_DIR_ID}' in parents and trashed = false"
    results = drive_service.files().list(q=query, fields="files(id)").execute()
    items = results.get('files', [])

    # B. 下載或建立
    if items:
        file_id = items[0]['id']
        request = drive_service.files().get_media(fileId=file_id)
        with io.FileIO(local_path, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
        wb = load_workbook(local_path)
        ws = wb.active
    else:
        wb = Workbook()
        ws = wb.active
        ws.title = "價量報表"
        ws["A1"], ws["B1"] = symbol, name
        ws["A2"], ws["A3"], ws["A4"], ws["A5"] = "融資", "融資差", "融券", "融券差"
        ws["A7"], ws["A8"], ws["A9"], ws["A10"] = "開", "高", "低", "收"
        ws["A12"], ws["B12"] = "成交價", "歷史成交量總和"
        file_id = None

    # C. 寫入與著色邏輯
    headers = [ws.cell(row=1, column=c).value for c in range(3, ws.max_column + 1)]
    if today not in headers:
        ws.cell(row=1, column=ws.max_column + 1).value = today
    today_col = headers.index(today) + 3 if today in headers else ws.max_column

    prev_col = today_col - 1 if today_col > 3 else None
    prev_fin = ws.cell(row=2, column=prev_col).value if prev_col else 0
    prev_short = ws.cell(row=4, column=prev_col).value if prev_col else 0

    ws.cell(row=2, column=today_col, value=margin_data["融資"])
    ws.cell(row=3, column=today_col, value=margin_data["融資"] - (prev_fin or 0))
    ws.cell(row=4, column=today_col, value=margin_data["融券"])
    ws.cell(row=5, column=today_col, value=margin_data["融券"] - (prev_short or 0))

    ws.cell(row=7, column=today_col, value=ohlc_data["開"])
    ws.cell(row=8, column=today_col, value=ohlc_data["高"])
    ws.cell(row=9, column=today_col, value=ohlc_data["低"])
    ws.cell(row=10, column=today_col, value=ohlc_data["收"])

    existing_prices = {float(ws.cell(row=r, column=1).value): r for r in range(13, ws.max_row + 1) if ws.cell(row=r, column=1).value}
    today_p_list = [p for p, _ in price_data]
    all_prices = sorted(set(existing_prices.keys()) | set(today_p_list), reverse=True)

    new_map = {}
    for i, price in enumerate(all_prices):
        row = i + 13
        if price not in existing_prices:
            ws.insert_rows(row)
            ws.cell(row=row, column=1, value=price)
            ws.cell(row=row, column=2, value=0)
        new_map[price] = row

    for price, vol in price_data:
        ws.cell(row=new_map[price], column=today_col, value=vol)

    # 著色邏輯 (簡單版 Heatmap)
    fill_none = PatternFill(fill_type=None)
    fill_top_5 = PatternFill(start_color="FFCCCC", end_color="FFCCCC", fill_type="solid")
    fill_close = PatternFill(start_color="00FFFF", end_color="00FFFF", fill_type="solid")

    for r in range(13, ws.max_row + 1):
        for c in range(1, ws.max_column + 1):
            ws.cell(row=r, column=c).fill = fill_none
        total = sum((ws.cell(row=r, column=c).value or 0) for c in range(3, ws.max_column + 1))
        ws.cell(row=r, column=2, value=total)

    # 標記成交量前 5
    sorted_rows = sorted(new_map.values(), key=lambda r: (ws.cell(row=r, column=2).value or 0), reverse=True)
    for r in sorted_rows[:5]:
        for c in range(1, ws.max_column + 1):
            ws.cell(row=r, column=c).fill = fill_top_5

    # 標記今日收盤價
    if float(ohlc_data["收"]) in new_map:
        ws.cell(row=new_map[float(ohlc_data["收"])], column=1).fill = fill_close

    wb.save(local_path)

    # D. 上傳回雲端
    media = MediaFileUpload(local_path, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    if file_id:
        drive_service.files().update(fileId=file_id, media_body=media).execute()
    else:
        file_metadata = {'name': file_name, 'parents': [BASE_DIR_ID]}
        drive_service.files().create(body=file_metadata, media_body=media).execute()
    print(f"✅ 已同步雲端: {file_name}")

# === 5. 主程式 ===
def main():
    perform_backup()
    stocks = get_all_taiwan_stocks()
    for symbol, name in stocks.items():
        try:
            p, m, o = fetch_price_by_volume(symbol), safe_fetch_margin_data(symbol), fetch_ohlc_data(symbol)
            if p['date'] == m['date'] == o['date']:
                update_excel_cloud(symbol, name, p, m, o)
                time.sleep(random.uniform(0.8, 1.2)) # 延遲避免封鎖
        except Exception as e:
            print(f"❌ {symbol} 失敗: {e}")

if __name__ == "__main__":
    main()
