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
# 從 GitHub Actions 的環境變數讀取金鑰
SERVICE_ACCOUNT_INFO = json.loads(os.environ.get('GCP_SERVICE_ACCOUNT_KEY'))
SCOPES = ['https://www.googleapis.com/auth/drive']
creds = service_account.Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO, scopes=SCOPES)
drive_service = build('drive', 'v3', credentials=creds)

# 【請務必填寫這裡】
BASE_DIR_ID = "1WE_tmOMxh9zffOVau4lAFOTnWDWFy_wJ"
BACKUP_DIR_ID = "11YNuJdVqUkAJTDhN4ASIkrxGDK3_PDUO"

# GitHub Actions 虛擬機內的臨時資料夾
LOCAL_TEMP = "./temp_stock"
if not os.path.exists(LOCAL_TEMP):
    os.makedirs(LOCAL_TEMP)

# === 2. 網路請求設定 ===
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
})

# === 3. 備份功能 (雲端複製版) ===
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
        drive_service.files().copy(fileId=f['id'], body={'parents': [new_folder_id]}).execute()
    print(f"✅ 雲端備份完成！共複製 {len(files)} 個檔案。")

# === 4. 股票抓取功能 (維持你原本的邏輯) ===
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

def fetch_price_by_volume(symbol):
    url = f"https://tw.stock.yahoo.com/_td-stock/api/resource/StockServices.priceByVolumes;symbol={symbol}.TW"
    res = session.get(url, timeout=10)
    js = res.json()
    # 簡化路徑抓取日期與價量，具體邏輯同你之前的代碼
    data = js.get('data', {}).get('priceByVolumes', [])
    date = js.get('data', {}).get('date', datetime.now().strftime("%Y-%m-%d"))[:10]
    return {"date": date, "data": [(float(d["price"]), int(d["volumeK"])) for d in data if d.get("volumeK")]}

def safe_fetch_margin_data(symbol):
    try:
        url = f"https://tw.stock.yahoo.com/_td-stock/api/resource/StockServices.creditsWithQuoteStats;limit=1;symbol={symbol}.TW"
        js = session.get(url).json()
        cred = js['data']['result']['credits'][0]
        return {"date": cred['date'][:10], "融資": int(cred['financingTotalVolK']), "融券": int(cred['shortTotalVolK'])}
    except:
        return {"date": datetime.now().strftime("%Y-%m-%d"), "融資": 0, "融券": 0}

def fetch_ohlc_data(symbol):
    url = f"https://tw.stock.yahoo.com/_td-stock/api/resource/StockServices.stockList;fields=avgPrice;symbols={symbol}.TW"
    item = session.get(url).json()['data']['list'][0]
    return {
        "date": item.get("regularMarketTime", "")[:10],
        "開": float(item['regularMarketOpen']),
        "高": float(item['regularMarketDayHigh']),
        "低": float(item['regularMarketDayLow']),
        "收": float(item['price'])
    }

# === 5. Excel 處理邏輯 (雲端同步版) ===
def update_excel_cloud(symbol, name, price_data, margin_data, ohlc_data):
    file_name = f"{symbol}_價量報表.xlsx"
    local_path = os.path.join(LOCAL_TEMP, file_name)
    
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
    else:
        wb = Workbook()
        file_id = None

    # C. 寫入資料 (此處省略你原本那一長串著色邏輯，請直接套用你原本 update_excel 的內容)
    ws = wb.active
    except FileNotFoundError:
        wb = Workbook()
        ws = wb.active
        ws.title = "價量報表"
        ws["A1"], ws["B1"] = symbol, name
        ws["A2"], ws["A3"], ws["A4"], ws["A5"] = "融資", "融資差", "融券", "融券差"
        ws["A7"], ws["A8"], ws["A9"], ws["A10"] = "開", "高", "低", "收"
        ws["A12"], ws["B12"] = "成交價", "歷史成交量總和"

    # 找出現有日期
    headers = [ws.cell(row=1, column=c).value for c in range(3, ws.max_column + 1)]
    if today not in headers:
        ws.cell(row=1, column=ws.max_column + 1).value = today
    today_col = headers.index(today) + 3 if today in headers else ws.max_column

    # 2️⃣ 計算融資差 / 融券差
    prev_col = today_col - 1 if today_col > 3 else None
    prev_fin, prev_short = None, None
    if prev_col:
        prev_fin = ws.cell(row=2, column=prev_col).value
        prev_short = ws.cell(row=4, column=prev_col).value

    fin_diff = (margin_data["融資"] - prev_fin) if prev_fin is not None else 0
    short_diff = (margin_data["融券"] - prev_short) if prev_short is not None else 0

    # 3️⃣ 寫入資券數據
    ws.cell(row=2, column=today_col, value=margin_data["融資"])
    ws.cell(row=3, column=today_col, value=fin_diff)
    ws.cell(row=4, column=today_col, value=margin_data["融券"])
    ws.cell(row=5, column=today_col, value=short_diff)

    # 4️⃣ 寫入 OHLC
    ws.cell(row=7, column=today_col, value=ohlc_data["開"])
    ws.cell(row=8, column=today_col, value=ohlc_data["高"])
    ws.cell(row=9, column=today_col, value=ohlc_data["低"])
    ws.cell(row=10, column=today_col, value=ohlc_data["收"])

    # 5️⃣ 更新價量資料
    existing_prices = {}
    for r in range(13, ws.max_row + 1):
        price = ws.cell(row=r, column=1).value
        if price:
            existing_prices[float(price)] = r

    today_prices = [p for p, _ in price_data]
    all_prices = sorted(set(existing_prices.keys()) | set(today_prices), reverse=True)

    current_row = 13
    new_map = {}
    for price in all_prices:
        if price not in existing_prices:
            ws.insert_rows(current_row)
            ws.cell(row=current_row, column=1, value=price)
            ws.cell(row=current_row, column=2, value=0)
        new_map[price] = current_row
        current_row += 1

    # 寫入今日成交量
    for price, vol in price_data:
        row = new_map[price]
        ws.cell(row=row, column=today_col, value=vol)

    # 重新計算歷史成交量總和
    for price in all_prices:
        row = new_map[price]
        total = 0
        for c in range(3, ws.max_column + 1):
            val = ws.cell(row=row, column=c).value
            total += val if isinstance(val, (int, float)) else 0
        ws.cell(row=row, column=2, value=total)

    # 6️⃣ 雙重著色 (成交量熱力圖)
    # [整列背景色] - 使用淺色系 (Pastel)，不干擾文字閱讀
    fill_row_top_1_5 = PatternFill(start_color="FFCCCC", end_color="FFCCCC", fill_type="solid")   # 淺紅
    fill_row_top_6_10 = PatternFill(start_color="FFFFCC", end_color="FFFFCC", fill_type="solid")  # 淺黃
    fill_row_top_11_20 = PatternFill(start_color="E0FFE0", end_color="E0FFE0", fill_type="solid") # 淺綠

    # [B欄強調色] - 使用飽和色，突顯單點大量
    fill_cell_top_1_5 = PatternFill(start_color="FF7F50", end_color="FF7F50", fill_type="solid") # 珊瑚紅/亮橘
    fill_cell_top_6_10 = PatternFill(start_color="FFD700", end_color="FFD700", fill_type="solid") # 金黃色

    fill_none = PatternFill(fill_type=None) # 無填滿 (白色)

    start_data_row = 13
    end_data_row = ws.max_row

    # ----------------------------------------------------
    # 第一階段：準備數據 & 清除舊顏色
    # ----------------------------------------------------
    avg_volume_list = []  # 存 (row, 平均值)
    raw_volume_list = []  # 存 (row, 原始值)

    for r in range(start_data_row, end_data_row + 1):
        # A. 先清除整列底色
        for c in range(1, ws.max_column + 1):
            ws.cell(row=r, column=c).fill = fill_none

        # B. 取得原始值 (B欄)
        raw_val = ws.cell(row=r, column=2).value
        if not isinstance(raw_val, (int, float)):
            raw_val = 0
        raw_volume_list.append((r, raw_val))

        # C. 計算平均值 (上下2列)
        window_start = max(start_data_row, r - 2)
        window_end = min(end_data_row, r + 2)
        window_values = []
        for wr in range(window_start, window_end + 1):
            val = ws.cell(row=wr, column=2).value
            window_values.append(val if isinstance(val, (int, float)) else 0)

        avg_val = sum(window_values) / len(window_values) if window_values else 0
        avg_volume_list.append((r, avg_val))

    # ----------------------------------------------------
    # 第二階段：根據「平均值」將 "整列" 上色
    # ----------------------------------------------------
    avg_volume_list.sort(key=lambda x: x[1], reverse=True) # 排序平均值

    # Top 1~5 名
    for i in range(min(5, len(avg_volume_list))):
        target_row = avg_volume_list[i][0]
        for c in range(1, ws.max_column + 1):
            ws.cell(row=target_row, column=c).fill = fill_row_top_1_5

    # Top 6~10 名
    for i in range(5, min(10, len(avg_volume_list))):
        target_row = avg_volume_list[i][0]
        for c in range(1, ws.max_column + 1):
            ws.cell(row=target_row, column=c).fill = fill_row_top_6_10

    # 11-20 名
    for i in range(10, min(20, len(avg_volume_list))):
        target_row = avg_volume_list[i][0]
        for c in range(1, ws.max_column + 1): ws.cell(row=target_row, column=c).fill = fill_row_top_11_20

    # ----------------------------------------------------
    # 第三階段：根據「原始值」將 "B欄" 重新上色 (覆蓋)
    # ----------------------------------------------------
    raw_volume_list.sort(key=lambda x: x[1], reverse=True) # 排序原始值

    # Top 1~5 原始 -> B欄粉色
    for i in range(min(5, len(raw_volume_list))):
        target_row = raw_volume_list[i][0]
        ws.cell(row=target_row, column=2).fill = fill_cell_top_1_5

    # Top 6~10 原始 -> B欄灰色
    for i in range(5, min(10, len(raw_volume_list))):
        target_row = raw_volume_list[i][0]
        ws.cell(row=target_row, column=2).fill = fill_cell_top_6_10

    # 7️⃣ 標記今日收盤價 (A欄)
    # 收盤價：青色 (最顯眼，代表目前位置)
    fill_close_price = PatternFill(start_color="00FFFF", end_color="00FFFF", fill_type="solid")
    # 開盤價：洋紅色 (與青色高對比，且不與成交量色系混淆)
    fill_open_price = PatternFill(start_color="FF00FF", end_color="FF00FF", fill_type="solid")

    # 取得價格數值 (轉為 float)
    close_price = float(ohlc_data["收"])
    open_price = float(ohlc_data["開"])

    # 1. 標記開盤價
    if open_price in new_map:
        target_row = new_map[open_price]
        ws.cell(row=target_row, column=1).fill = fill_open_price
    #    print(f"🔹 開盤價 {open_price} 已標記於 Row {target_row} (洋紅色)")
    #else:
    #    print(f"⚠️ 找不到開盤價 {open_price}")

    # 2. 標記收盤價 (放在後面執行，若開收同價，收盤色會覆蓋開盤色)
    if close_price in new_map:
        target_row = new_map[close_price]
        ws.cell(row=target_row, column=1).fill = fill_close_price
    #    print(f"🎯 收盤價 {close_price} 已標記於 Row {target_row} (青色)")
    #else:
    #    print(f"⚠️ 找不到收盤價 {close_price}")
    wb.save(local_path)

    # D. 上傳回雲端
    media = MediaFileUpload(local_path, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    if file_id:
        drive_service.files().update(fileId=file_id, media_body=media).execute()
    else:
        file_metadata = {'name': file_name, 'parents': [BASE_DIR_ID]}
        drive_service.files().create(body=file_metadata, media_body=media).execute()
    print(f"✅ 已同步雲端: {file_name}")

# === 6. 主程式 ===
def main():
    perform_backup()
    stocks = get_all_taiwan_stocks()
    for symbol, name in stocks.items():
        try:
            p, m, o = fetch_price_by_volume(symbol), safe_fetch_margin_data(symbol), fetch_ohlc_data(symbol)
            if p['date'] == m['date'] == o['date']:
                update_excel_cloud(symbol, name, p, m, o)
                time.sleep(random.uniform(0.5, 1.0))
        except Exception as e:
            print(f"❌ {symbol} 失敗: {e}")

if __name__ == "__main__":
    main()
