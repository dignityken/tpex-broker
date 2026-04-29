import requests
from bs4 import BeautifulSoup
import json
import os
import re
import sys
import urllib.parse
from datetime import datetime, timedelta, timezone, date as date_type

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept-Language': 'zh-TW,zh;q=0.9',
    'Referer': 'https://www.tpex.org.tw/',
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


def fetch_url(url):
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.encoding = 'utf-8'
    return resp.text


def fetch_live(mode):
    stocks = parse_html(fetch_url(LIVE_URLS[mode]))
    now = datetime.now(TW_TZ)
    return {'updated_at': now.strftime('%Y/%m/%d %H:%M'), 'unit': UNITS[mode], 'stocks': stocks}


def fetch_historical(day: date_type, mode):
    date_str = day.strftime('%Y/%m/%d')
    url = HIST_URLS[mode].format(date=urllib.parse.quote(date_str, safe=''))
    stocks = parse_html(fetch_url(url))
    return {'date': day.strftime('%Y-%m-%d'), 'unit': UNITS[mode], 'stocks': stocks}


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
            except Exception as e:
                print(f'  {mode} 失敗：{e}')
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

    # 2. 盤中快照
    intraday_dir = f'data/intraday/{date_key}'
    os.makedirs(intraday_dir, exist_ok=True)
    for mode in ['amt', 'vol']:
        snap = dict(live_data[mode])
        snap['snapshot_time'] = now.strftime('%H:%M')
        with open(f'{intraday_dir}/{hour_key}-{mode}.json', 'w', encoding='utf-8') as f:
            json.dump(snap, f, ensure_ascii=False, indent=2)
    update_intraday_manifest(date_key, hour_key)
    print(f'[快照] {date_key}/{hour_key} 已儲存')

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
