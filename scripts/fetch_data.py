import requests
from bs4 import BeautifulSoup
import json
import re
import os
from datetime import datetime, timezone, timedelta

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept-Language': 'zh-TW,zh;q=0.9',
    'Referer': 'https://www.tpex.org.tw/',
}

SOURCES = {
    'amt': {
        'url': 'https://www.tpex.org.tw/www/zh-tw/mostActive/brokerAmt?id=&response=html',
        'unit': '千元',
    },
    'vol': {
        'url': 'https://www.tpex.org.tw/www/zh-tw/mostActive/brokerVol?id=&response=html',
        'unit': '張',
    },
}


def parse_number(s):
    s = str(s).strip().replace(',', '').replace(' ', '')
    if not s or s in ('-', 'FALSE', 'false'):
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def fetch_and_parse(mode):
    source = SOURCES[mode]
    resp = requests.get(source['url'], headers=HEADERS, timeout=30)
    resp.encoding = 'utf-8'
    soup = BeautifulSoup(resp.text, 'lxml')

    # 找含有券商資料的主表格
    main_table = None
    for t in soup.find_all('table'):
        text = t.get_text()
        if '元大' in text and '凱基' in text and '富邦' in text:
            main_table = t
            break

    if not main_table:
        raise ValueError(f'找不到主表格 ({mode})')

    stocks = []
    current_stock = None

    for row in main_table.find_all('tr'):
        cells = row.find_all(['td', 'th'])
        if not cells:
            continue

        texts = [c.get_text(strip=True) for c in cells]
        full_text = ' '.join(texts)

        # 略過欄位標題列
        if texts[0] in ('排行', ''):
            continue

        # 判斷是否為股票標題列（格式：數字 - 名稱(代號)）
        stock_match = re.search(r'(\d+)\s*[-－]\s*(.+?)\((\d+[A-Z*]*)\)', full_text)
        is_header = stock_match and (len(cells) <= 2 or not texts[0].isdigit())

        if is_header:
            if current_stock:
                stocks.append(current_stock)
            m = stock_match
            current_stock = {
                'rank': int(m.group(1)),
                'name': m.group(2).strip(),
                'code': m.group(3),
                'brokers': [],
            }
            continue

        # 券商資料列（第一欄是 1~10 的數字）
        if current_stock and len(texts) >= 4 and texts[0].isdigit():
            buy = parse_number(texts[2])
            sell = parse_number(texts[3])
            if buy is not None and sell is not None:
                current_stock['brokers'].append({
                    'rank': int(texts[0]),
                    'name': texts[1],
                    'buy': buy,
                    'sell': sell,
                    'net': buy - sell,
                })

    if current_stock:
        stocks.append(current_stock)

    # 計算各股合計買賣超
    for stock in stocks:
        stock['total_net'] = sum(b['net'] for b in stock['brokers']) if stock['brokers'] else 0

    tw_tz = timezone(timedelta(hours=8))
    now = datetime.now(tw_tz)

    return {
        'updated_at': now.strftime('%Y/%m/%d %H:%M'),
        'unit': source['unit'],
        'stocks': stocks,
    }


if __name__ == '__main__':
    os.makedirs('data', exist_ok=True)
    for mode in ['amt', 'vol']:
        print(f'抓取 {mode}...')
        try:
            data = fetch_and_parse(mode)
            with open(f'data/{mode}.json', 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f'完成：{len(data["stocks"])} 支股票 → data/{mode}.json')
        except Exception as e:
            print(f'失敗：{e}')
            raise
