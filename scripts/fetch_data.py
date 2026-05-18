import requests
from bs4 import BeautifulSoup
import json
import os
import re
import sys
import time
import urllib.parse
from datetime import datetime, timedelta, timezone, date as date_type

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept-Language': 'zh-TW,zh;q=0.9',
    'Referer': 'https://www.tpex.org.tw/',
}
YF_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
}

LIVE_URLS = {
    'amt': 'https://www.tpex.org.tw/www/zh-tw/mostActive/brokerAmt?id=&response=html',
    'vol': 'https://www.tpex.org.tw/www/zh-tw/mostActive/brokerVol?id=&response=html',
}
HIST_URLS = {
    'amt': 'https://www.tpex.org.tw/www/zh-tw/afterTrading/brokerAmt?date={date}&id=&response=html',
    'vol': 'https://www.tpex.org.tw/www/zh-tw/afterTrading/brokerVol?date={date}&id=&response=html',
}
UNITS = {'amt': '千元', 'vol': '張'}
TW_TZ = timezone(timedelta(hours=8))
PAT = re.compile(r'(\d+)\s*[-－]\s*(.+?)\((\d+[A-Z*]*)\)')


def parse_num(s):
    s = str(s).strip().replace(',', '')
    if not s or s in ('-', 'FALSE', 'false'):
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def parse_html(html_text):
    soup = BeautifulSoup(html_text, 'lxml')
    main_table = None
    for t in soup.find_all('table'):
        txt = t.get_text()
        if '元大' in txt and '凱基' in txt and '富邦' in txt:
            main_table = t
            break
    if not main_table:
        return []

    stocks, cur = [], None
    for row in main_table.find_all('tr'):
        cells = row.find_all(['td', 'th'])
        if not cells:
            continue
        texts = [c.get_text(strip=True) for c in cells]
        if texts[0] in ('排行', ''):
            continue

        m = PAT.search(' '.join(texts))
        is_hdr = m and (len(cells) <= 2 or not texts[0].isdigit())

        if is_hdr:
            if cur:
                stocks.append(cur)
            cur = {'rank': int(m.group(1)), 'name': m.group(2).strip(),
                   'code': m.group(3), 'brokers': []}
            continue

        if cur and len(texts) >= 4 and texts[0].isdigit():
            buy, sell = parse_num(texts[2]), parse_num(texts[3])
            if buy is not None and sell is not None:
                cur['brokers'].append({
                    'rank': int(texts[0]), 'name': texts[1],
                    'buy': buy, 'sell': sell, 'net': buy - sell,
                })

    if cur:
        stocks.append(cur)
    for s in stocks:
        s['total_net'] = sum(b['net'] for b in s['brokers']) if s['brokers'] else 0
    return stocks


def fetch_url(url, retries=3, delay=15):
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.encoding = 'utf-8'
            return resp.text
        except Exception as e:
            if attempt < retries - 1:
                print(f'  網路錯誤，{delay}秒後重試（{attempt+1}/{retries}）：{e}')
                time.sleep(delay)
            else:
                raise


def fetch_live(mode, retries=3, delay=20):
    for attempt in range(retries):
        stocks = parse_html(fetch_url(LIVE_URLS[mode]))
        now = datetime.now(TW_TZ)
        if stocks:
            return {'updated_at': now.strftime('%Y/%m/%d %H:%M'), 'unit': UNITS[mode], 'stocks': stocks}
        if attempt < retries - 1:
            print(f'  {mode}: 取得空資料，{delay}秒後重試（{attempt+1}/{retries}）...')
            time.sleep(delay)
    print(f'  {mode}: 重試後仍無資料，略過快照')
    return {'updated_at': now.strftime('%Y/%m/%d %H:%M'), 'unit': UNITS[mode], 'stocks': []}


def fetch_historical(day: date_type, mode):
    date_str = day.strftime('%Y/%m/%d')
    url = HIST_URLS[mode].format(date=urllib.parse.quote(date_str, safe=''))
    stocks = parse_html(fetch_url(url))
    return {'date': day.strftime('%Y-%m-%d'), 'unit': UNITS[mode], 'stocks': stocks}


def fetch_kline_yahoo(code, date_str):
    """抓 Yahoo Finance 60 分 K 線，date_str = YYYY-MM-DD"""
    symbol = f'{code}.TWO'
    day = datetime.strptime(date_str, '%Y-%m-%d')
    period1 = int(day.replace(hour=8,  minute=0, tzinfo=TW_TZ).timestamp())
    period2 = int(day.replace(hour=14, minute=0, tzinfo=TW_TZ).timestamp())
    url = (f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}'
           f'?interval=60m&period1={period1}&period2={period2}&includePrePost=false')
    try:
        resp = requests.get(url, headers=YF_HEADERS, timeout=15)
        data = resp.json()
        result = data.get('chart', {}).get('result') or []
        if not result:
            return []
        r = result[0]
        timestamps = r.get('timestamp', [])
        q = r['indicators']['quote'][0]
        bars = []
        for i, ts in enumerate(timestamps):
            o, h, l, c = q['open'][i], q['high'][i], q['low'][i], q['close'][i]
            if None in (o, h, l, c):
                continue
            tw_t = datetime.fromtimestamp(ts, TW_TZ)
            bars.append({'time': tw_t.strftime('%H:%M'),
                         'open': round(o, 2), 'high': round(h, 2),
                         'low':  round(l, 2), 'close': round(c, 2)})
        return bars
    except Exception as e:
        print(f'    K線 {code} 失敗：{e}')
        return []


def save_klines(codes, date_str, skip_existing=False):
    os.makedirs('data/kline', exist_ok=True)
    ok = 0
    for code in sorted(codes):
        path = f'data/kline/{date_str}-{code}.json'
        if skip_existing and os.path.exists(path):
            ok += 1
            continue
        bars = fetch_kline_yahoo(code, date_str)
        if bars:
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(bars, f, ensure_ascii=False)
            ok += 1
        time.sleep(0.3)
    print(f'  K線：{ok}/{len(codes)} 支完成')


def get_recent_weekdays(n=15):
    today = datetime.now(TW_TZ).date()
    days, d = [], today - timedelta(days=1)
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d)
        d -= timedelta(days=1)
    return days


def update_intraday_manifest(date_key, hour_key):
    path = 'data/intraday/manifest.json'
    manifest = {}
    if os.path.exists(path):
        with open(path, encoding='utf-8') as f:
            manifest = json.load(f)
    if date_key not in manifest:
        manifest[date_key] = []
    if hour_key not in manifest[date_key]:
        manifest[date_key].append(hour_key)
        manifest[date_key].sort()
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)


# ── 環境變數：指定歷史日期模式 ──
HIST_DATE = os.environ.get('HIST_DATE', '').strip()


if __name__ == '__main__':
    os.makedirs('data', exist_ok=True)
    os.makedirs('data/hist', exist_ok=True)
    os.makedirs('data/intraday', exist_ok=True)

    # ── 模式一：指定歷史日期（由 HTML 查詢觸發）──
    if HIST_DATE:
        print(f'[指定歷史] 抓取 {HIST_DATE}...')
        try:
            day = datetime.strptime(HIST_DATE, '%Y-%m-%d').date()
        except ValueError:
            print(f'日期格式錯誤：{HIST_DATE}')
            sys.exit(1)

        codes = set()
        for mode in ['amt', 'vol']:
            path = f'data/hist/{HIST_DATE}-{mode}.json'
            try:
                data = fetch_historical(day, mode)
                if not data['stocks']:
                    print(f'  {mode}: 無資料（假日？）')
                else:
                    with open(path, 'w', encoding='utf-8') as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                    print(f'  {mode}: {len(data["stocks"])} 支')
                    codes.update(s['code'] for s in data['stocks'])
            except Exception as e:
                print(f'  {mode} 失敗：{e}')
        if codes:
            print(f'[K線] 抓取歷史 {HIST_DATE}...')
            save_klines(codes, HIST_DATE, skip_existing=True)
        sys.exit(0)

    # ── 模式二：一般排程執行 ──
    now = datetime.now(TW_TZ)
    date_key = now.strftime('%Y-%m-%d')
    hour_key = now.strftime('%H')

    # 1. 即時資料
    live_data = {}
    for mode in ['amt', 'vol']:
        print(f'[即時] 抓取 {mode}...')
        try:
            data = fetch_live(mode)
            live_data[mode] = data
            with open(f'data/{mode}.json', 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f'  完成：{len(data["stocks"])} 支')
        except Exception as e:
            print(f'  失敗：{e}')
            raise

    # 2. 盤中快照（有資料才存）
    intraday_dir = f'data/intraday/{date_key}'
    os.makedirs(intraday_dir, exist_ok=True)
    snap_ok = all(live_data.get(m, {}).get('stocks') for m in ['amt', 'vol'])
    if snap_ok:
        for mode in ['amt', 'vol']:
            snap = dict(live_data[mode])
            snap['snapshot_time'] = now.strftime('%H:%M')
            with open(f'{intraday_dir}/{hour_key}-{mode}.json', 'w', encoding='utf-8') as f:
                json.dump(snap, f, ensure_ascii=False, indent=2)
        update_intraday_manifest(date_key, hour_key)
        print(f'[快照] {date_key}/{hour_key} 已儲存')
    else:
        print(f'[快照] {date_key}/{hour_key} 資料不完整，略過')

    # 3. 近期歷史資料（只補還沒有的）
    available = []
    for day in get_recent_weekdays(15):
        key = day.strftime('%Y-%m-%d')
        amt_path = f'data/hist/{key}-amt.json'
        vol_path = f'data/hist/{key}-vol.json'

        if os.path.exists(amt_path) and os.path.exists(vol_path):
            available.append(key)
            continue

        print(f'[歷史] 抓取 {key}...')
        ok = True
        for mode in ['amt', 'vol']:
            path = f'data/hist/{key}-{mode}.json'
            if os.path.exists(path):
                continue
            try:
                data = fetch_historical(day, mode)
                if not data['stocks']:
                    print(f'  {mode}: 無資料（假日）')
                    ok = False
                    break
                with open(path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                print(f'  {mode}: {len(data["stocks"])} 支')
            except Exception as e:
                print(f'  {mode} 失敗：{e}')
                ok = False
                break
        if ok:
            available.append(key)

    with open('data/hist/manifest.json', 'w', encoding='utf-8') as f:
        json.dump(sorted(available, reverse=True), f, ensure_ascii=False, indent=2)
    print(f'[清單] 歷史可用：{len(available)} 天')

    # 4. 今日 K 線
    codes = set()
    for mode in ['amt', 'vol']:
        for s in live_data.get(mode, {}).get('stocks', []):
            codes.add(s['code'])
    if codes:
        print(f'[K線] 抓取今日 {date_key}...')
        save_klines(codes, date_key, skip_existing=False)
