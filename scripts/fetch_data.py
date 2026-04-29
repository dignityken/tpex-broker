import requests
from bs4 import BeautifulSoup
import json
import os
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

UNITS = { 'amt': '千元', 'vol': '張' }

TW_TZ = timezone(timedelta(hours=8))


def parse_num(s):
    s = str(s).strip().replace(',', '').replace(' ', '')
    if not s or s in ('-', 'FALSE', 'false'):
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def parse_html(html_text, mode):
    soup = BeautifulSoup(html_text, 'lxml')
    main_table = None
    for t in soup.find_all('table'):
        txt = t.get_text()
        if '元大' in txt and '凱基' in txt and '富邦' in txt:
            main_table = t
            break
    if not main_table:
        return []

    import re
    stocks, cur = [], None
    pat = re.compile(r'(\d+)\s*[-－]\s*(.+?)\((\d+[A-Z*]*)\)')

    for row in main_table.find_all('tr'):
        cells = row.find_all(['td', 'th'])
        if not cells:
            continue
        texts = [c.get_text(strip=True) for c in cells]
        if texts[0] in ('排行', ''):
            continue

        m = pat.search(' '.join(texts))
        is_hdr = m and (len(cells) <= 2 or not texts[0].isdigit())

        if is_hdr:
            if cur:
                stocks.append(cur)
            cur = { 'rank': int(m.group(1)), 'name': m.group(2).strip(), 'code': m.group(3), 'brokers': [] }
            continue

        if cur and len(texts) >= 4 and texts[0].isdigit():
            buy = parse_num(texts[2])
            sell = parse_num(texts[3])
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


# ── 即時資料 ──
def fetch_live(mode):
    html = fetch_url(LIVE_URLS[mode])
    stocks = parse_html(html, mode)
    now = datetime.now(TW_TZ)
    return {
        'updated_at': now.strftime('%Y/%m/%d %H:%M'),
        'unit': UNITS[mode],
        'stocks': stocks,
    }


# ── 歷史資料（單日）──
def fetch_historical(day: date_type, mode):
    date_str = day.strftime('%Y/%m/%d')
    date_enc = urllib.parse.quote(date_str, safe='')
    url = HIST_URLS[mode].format(date=date_enc)
    html = fetch_url(url)
    stocks = parse_html(html, mode)
    return {
        'date': day.strftime('%Y-%m-%d'),
        'unit': UNITS[mode],
        'stocks': stocks,
    }


def get_recent_weekdays(n=15):
    """回傳最近 n 個平日（不含今天）"""
    today = datetime.now(TW_TZ).date()
    days, d = [], today - timedelta(days=1)
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d)
        d -= timedelta(days=1)
    return days


if __name__ == '__main__':
    os.makedirs('data', exist_ok=True)
    os.makedirs('data/hist', exist_ok=True)

    # ── 1. 即時資料 ──
    for mode in ['amt', 'vol']:
        print(f'[即時] 抓取 {mode}...')
        try:
            data = fetch_live(mode)
            with open(f'data/{mode}.json', 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f'  完成：{len(data["stocks"])} 支股票')
        except Exception as e:
            print(f'  失敗：{e}')
            raise

    # ── 2. 近期歷史資料（只抓還沒存的日期）──
    recent_days = get_recent_weekdays(15)
    available = []

    for day in recent_days:
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
                    print(f'  {mode}: 無資料（假日？）')
                    ok = False
                    break
                with open(path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                print(f'  {mode}: {len(data["stocks"])} 支股票')
            except Exception as e:
                print(f'  {mode} 失敗：{e}')
                ok = False
                break

        if ok:
            available.append(key)

    # ── 3. 儲存可用日期清單 ──
    available_sorted = sorted(available, reverse=True)
    with open('data/hist/manifest.json', 'w', encoding='utf-8') as f:
        json.dump(available_sorted, f, ensure_ascii=False)
    print(f'[清單] 可用歷史日期：{len(available_sorted)} 天')
