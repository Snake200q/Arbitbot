import asyncio
import requests
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from telegram.error import BadRequest, TimedOut

logging.basicConfig(level=logging.INFO)

BOT_TOKEN = "8518793041:AAERxxCHu-LTaG6Wf90ER2GPrnZamkcWgYc"
CHAT_ID = "942607751"
CHECK_INTERVAL = 120
MIN_SPREAD = 0.1
MAX_RESULTS = 25

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}

def safe_get(url, params=None, timeout=20, extra_headers=None):
    try:
        h = {**HEADERS, **(extra_headers or {})}
        r = requests.get(url, params=params, headers=h, timeout=timeout)
        if r.status_code == 200:
            return r.json()
        print(f"HTTP {r.status_code} => {url}")
    except Exception as e:
        print(f"ERR {url} => {e}")
    return None

def get_polymarket():
    results = []
    try:
        next_cursor = ""
        for _ in range(20):
            params = {"active": "true", "closed": "false", "limit": 100}
            if next_cursor:
                params["next_cursor"] = next_cursor
            data = safe_get("https://clob.polymarket.com/markets", params=params)
            if not data:
                break
            for m in data.get("data", []):
                try:
                    tokens = m.get("tokens", [])
                    if len(tokens) != 2:
                        continue
                    yes_p = no_p = None
                    for t in tokens:
                        out = t.get("outcome", "").upper()
                        price = float(t.get("price", 0))
                        if out == "YES":
                            yes_p = price
                        elif out == "NO":
                            no_p = price
                    if not yes_p or not no_p:
                        continue
                    if yes_p < 0.01 or no_p < 0.01:
                        continue
                    total = yes_p + no_p
                    spread = round((1 - total) * 100, 2)
                    slug = m.get("market_slug") or m.get("slug") or m.get("condition_id", "")
                    cid = m.get("condition_id", "")
                    results.append({
                        "platform": "Polymarket",
                        "question": m.get("question", "")[:120],
                        "yes": round(yes_p * 100, 1),
                        "no": round(no_p * 100, 1),
                        "total": round(total * 100, 1),
                        "spread": spread,
                        "url": f"https://polymarket.com/event/{slug}",
                        "volume": float(m.get("volume", 0) or 0),
                        "cid": cid
                    })
                except:
                    continue
            nc = data.get("next_cursor", "")
            if not nc or nc == "LTE=":
                break
            next_cursor = nc
    except Exception as e:
        print(f"[Polymarket] {e}")
    print(f"[Polymarket] {len(results)}")
    return results

def get_polymarket_gamma():
    results = []
    try:
        offset = 0
        while True:
            params = {
                "active": "true", "closed": "false",
                "limit": 100, "offset": offset,
                "order": "volume24hr", "ascending": "false"
            }
            data = safe_get("https://gamma-api.polymarket.com/markets", params=params, timeout=25)
            if not data:
                break
            markets = data if isinstance(data, list) else data.get("markets", [])
            if not markets:
                break
            for m in markets:
                try:
                    out_prices = m.get("outcomePrices")
                    if not out_prices:
                        continue
                    if isinstance(out_prices, str):
                        out_prices = json.loads(out_prices)
                    if len(out_prices) != 2:
                        continue
                    yes_p = float(out_prices[0])
                    no_p = float(out_prices[1])
                    if yes_p < 0.01 or no_p < 0.01:
                        continue
                    total = yes_p + no_p
                    spread = round((1 - total) * 100, 2)
                    slug = m.get("slug") or str(m.get("id", ""))
                    cid = m.get("conditionId") or m.get("condition_id") or ""
                    results.append({
                        "platform": "Polymarket",
                        "question": m.get("question", "")[:120],
                        "yes": round(yes_p * 100, 1),
                        "no": round(no_p * 100, 1),
                        "total": round(total * 100, 1),
                        "spread": spread,
                        "url": f"https://polymarket.com/event/{slug}",
                        "volume": float(m.get("volume24hr") or m.get("volume") or 0),
                        "cid": cid
                    })
                except:
                    continue
            if len(markets) < 100:
                break
            offset += 100
    except Exception as e:
        print(f"[Poly Gamma] {e}")
    print(f"[Poly Gamma] {len(results)}")
    return results

def get_manifold():
    results = []
    try:
        before = ""
        for _ in range(3):
            params = {"limit": 100, "sort": "liquidity", "filter": "open"}
            if before:
                params["before"] = before
            data = safe_get("https://api.manifold.markets/v0/markets", params=params)
            if not data or len(data) == 0:
                break
            for m in data:
                try:
                    if m.get("outcomeType") != "BINARY":
                        continue
                    prob = float(m.get("probability", 0))
                    if prob <= 0.02 or prob >= 0.98:
                        continue
                    yes_p = prob
                    no_p = 1 - prob
                    eff = yes_p + no_p - 0.02
                    spread = round((1 - eff) * 100, 2)
                    results.append({
                        "platform": "Manifold",
                        "question": m.get("question", "")[:120],
                        "yes": round(yes_p * 100, 1),
                        "no": round(no_p * 100, 1),
                        "total": round(eff * 100, 1),
                        "spread": spread,
                        "url": m.get("url", ""),
                        "volume": float(m.get("volume", 0) or 0),
                        "cid": ""
                    })
                except:
                    continue
            if len(data) < 100:
                break
            before = data[-1].get("id", "")
    except Exception as e:
        print(f"[Manifold] {e}")
    print(f"[Manifold] {len(results)}")
    return results

def get_kalshi():
    results = []
    try:
        cursor = ""
        for _ in range(10):
            params = {"status": "open", "limit": 100}
            if cursor:
                params["cursor"] = cursor
            data = safe_get("https://trading-api.kalshi.com/trade-api/v2/markets", params=params)
            if not data:
                break
            for m in data.get("markets", []):
                try:
                    yb = m.get("yes_bid") or 0
                    ya = m.get("yes_ask") or 0
                    nb = m.get("no_bid") or 0
                    na = m.get("no_ask") or 0
                    if yb == 0 and ya == 0:
                        continue
                    ym = (yb + ya) / 2 if ya > 0 else yb
                    nm = (nb + na) / 2 if na > 0 else nb
                    if ym <= 0 or nm <= 0:
                        continue
                    total = (ym + nm) / 100
                    spread = round((1 - total) * 100, 2)
                    ticker = m.get("ticker", "")
                    results.append({
                        "platform": "Kalshi",
                        "question": m.get("title", "")[:120],
                        "yes": round(ym, 1),
                        "no": round(nm, 1),
                        "total": round(total * 100, 1),
                        "spread": spread,
                        "url": f"https://kalshi.com/markets/{ticker}",
                        "volume": float(m.get("volume") or 0),
                        "cid": ""
                    })
                except:
                    continue
            cursor = data.get("cursor", "")
            if not cursor:
                break
    except Exception as e:
        print(f"[Kalshi] {e}")
    print(f"[Kalshi] {len(results)}")
    return results

def get_predictit():
    results = []
    try:
        data = safe_get("https://www.predictit.org/api/marketdata/all/", timeout=20)
        if not data:
            return results
        for m in data.get("markets", []):
            try:
                for c in m.get("contracts", []):
                    yb = c.get("bestBuyYesCost")
                    nb = c.get("bestBuyNoCost")
                    if not yb or not nb:
                        continue
                    yb, nb = float(yb), float(nb)
                    if yb < 0.02 or nb < 0.02:
                        continue
                    total = yb + nb
                    spread = round((1 - total) * 100, 2)
                    cname = c.get("name", "")
                    if cname.lower() in ["yes", "no", ""]:
                        question = m.get("name", "")[:120]
                    else:
                        question = f"{m.get('name', '')} — {cname}"[:120]
                    results.append({
                        "platform": "PredictIt",
                        "question": question,
                        "yes": round(yb * 100, 1),
                        "no": round(nb * 100, 1),
                        "total": round(total * 100, 1),
                        "spread": spread,
                        "url": f"https://www.predictit.org/markets/detail/{m.get('id', '')}",
                        "volume": 0,
                        "cid": ""
                    })
            except:
                continue
    except Exception as e:
        print(f"[PredictIt] {e}")
    print(f"[PredictIt] {len(results)}")
    return results

def get_limitless():
    results = []
    try:
        h = {"Origin": "https://limitless.exchange", "Referer": "https://limitless.exchange/"}
        data = safe_get("https://limitless.exchange/api/v1/markets", extra_headers=h, timeout=20)
        if not data:
            return results
        markets = data if isinstance(data, list) else data.get("markets", data.get("data", []))
        for m in markets:
            try:
                prices = m.get("prices") or m.get("outcomePrices") or []
                if isinstance(prices, str):
                    prices = json.loads(prices)
                if prices and len(prices) >= 2:
                    yes_p = float(prices[0])
                    no_p = float(prices[1])
                else:
                    yes_p = float(m.get("yes_price") or m.get("yesPrice") or 0)
                    no_p = float(m.get("no_price") or m.get("noPrice") or 0)
                if yes_p > 1:
                    yes_p /= 100
                if no_p > 1:
                    no_p /= 100
                if yes_p < 0.01 or no_p < 0.01:
                    continue
                total = yes_p + no_p
                spread = round((1 - total) * 100, 2)
                cid = m.get("conditionId") or m.get("condition_id") or ""
                mid = m.get("id") or m.get("address") or ""
                results.append({
                    "platform": "Limitless",
                    "question": str(m.get("title") or m.get("question") or "")[:120],
                    "yes": round(yes_p * 100, 1),
                    "no": round(no_p * 100, 1),
                    "total": round(total * 100, 1),
                    "spread": spread,
                    "url": f"https://limitless.exchange/markets/{mid}",
                    "volume": float(m.get("volume") or 0),
                    "cid": cid
                })
            except:
                continue
    except Exception as e:
        print(f"[Limitless] {e}")
    print(f"[Limitless] {len(results)}")
    return results

def get_opinion():
    results = []
    urls = [
        "https://opinion-market.com/api/v1/markets?status=open&limit=100",
        "https://opinion-market.com/api/markets?status=open",
        "https://api.opinion-market.com/v1/markets?status=open",
    ]
    for url in urls:
        h = {"Origin": "https://opinion-market.com", "Referer": "https://opinion-market.com/"}
        data = safe_get(url, extra_headers=h, timeout=15)
        if not data:
            continue
        markets = data if isinstance(data, list) else data.get("markets", data.get("data", []))
        for m in markets:
            try:
                yes_p = float(m.get("yes_price") or m.get("yesPrice") or m.get("yes") or 0)
                no_p = float(m.get("no_price") or m.get("noPrice") or m.get("no") or 0)
                if yes_p > 1:
                    yes_p /= 100
                if no_p > 1:
                    no_p /= 100
                if yes_p < 0.01 or no_p < 0.01:
                    continue
                total = yes_p + no_p
                spread = round((1 - total) * 100, 2)
                cid = m.get("conditionId") or m.get("condition_id") or ""
                mid = m.get("id") or ""
                results.append({
                    "platform": "Opinion",
                    "question": str(m.get("title") or m.get("question") or "")[:120],
                    "yes": round(yes_p * 100, 1),
                    "no": round(no_p * 100, 1),
                    "total": round(total * 100, 1),
                    "spread": spread,
                    "url": f"https://opinion-market.com/markets/{mid}",
                    "volume": float(m.get("volume") or 0),
                    "cid": cid
                })
            except:
                continue
        if results:
            break
    print(f"[Opinion] {len(results)}")
    return results

def get_predictfun():
    results = []
    urls = [
        "https://api.predict.fun/api/v1/markets?status=open&limit=200",
        "https://predict.fun/api/v1/markets?status=open&limit=200",
        "https://predict.fun/api/markets?status=open",
    ]
    for url in urls:
        h = {"Origin": "https://predict.fun", "Referer": "https://predict.fun/"}
        data = safe_get(url, extra_headers=h, timeout=15)
        if not data:
            continue
        markets = data if isinstance(data, list) else data.get("markets", data.get("data", []))
        for m in markets:
            try:
                yes_p = float(m.get("yes_price") or m.get("yesPrice") or m.get("yes") or 0)
                no_p = float(m.get("no_price") or m.get("noPrice") or m.get("no") or 0)
                if yes_p > 1:
                    yes_p /= 100
                if no_p > 1:
                    no_p /= 100
                if yes_p < 0.01 or no_p < 0.01:
                    continue
                total = yes_p + no_p
                spread = round((1 - total) * 100, 2)
                cid = m.get("conditionId") or m.get("condition_id") or ""
                mid = m.get("id") or ""
                results.append({
                    "platform": "PredictFun",
                    "question": str(m.get("question") or m.get("title") or "")[:120],
                    "yes": round(yes_p * 100, 1),
                    "no": round(no_p * 100, 1),
                    "total": round(total * 100, 1),
                    "spread": spread,
                    "url": f"https://predict.fun/markets/{mid}",
                    "volume": float(m.get("volume") or 0),
                    "cid": cid
                })
            except:
                continue
        if results:
            break
    print(f"[PredictFun] {len(results)}")
    return results

def merge_poly(clob, gamma):
    seen = set()
    merged = []
    for m in clob:
        cid = m.get("cid", "")
        if cid:
            seen.add(cid)
        merged.append(m)
    for m in gamma:
        cid = m.get("cid", "")
        if cid and cid in seen:
            continue
        merged.append(m)
        if cid:
            seen.add(cid)
    return merged

def find_cross_cid(all_markets):
    by_cid = {}
    for m in all_markets:
        cid = m.get("cid", "")
        if not cid:
            continue
        by_cid.setdefault(cid, []).append(m)
    cross = []
    for cid, markets in by_cid.items():
        if len(markets) < 2:
            continue
        for i in range(len(markets)):
            for j in range(i + 1, len(markets)):
                a, b = markets[i], markets[j]
                if a["platform"] == b["platform"]:
                    continue
                diff = max(abs(a["yes"] - b["yes"]), abs(a["no"] - b["no"]))
                if diff >= MIN_SPREAD:
                    cross.append({
                        "question": a["question"][:100],
                        "platform_a": a["platform"], "yes_a": a["yes"], "no_a": a["no"], "url_a": a["url"],
                        "platform_b": b["platform"], "yes_b": b["yes"], "no_b": b["no"], "url_b": b["url"],
                        "diff": round(diff, 1),
                        "volume": max(a.get("volume", 0), b.get("volume", 0)),
                        "exact": True
                    })
    return cross

def find_cross_text(all_markets):
    cross = []
    checked = set()
    for i, a in enumerate(all_markets):
        for j, b in enumerate(all_markets):
            if i >= j or a["platform"] == b["platform"]:
                continue
            key = tuple(sorted([i, j]))
            if key in checked:
                continue
            checked.add(key)
            wa = set(w.lower() for w in a["question"].split() if len(w) > 4)
            wb = set(w.lower() for w in b["question"].split() if len(w) > 4)
            if len(wa & wb) < 4:
                continue
            diff = max(abs(a["yes"] - b["yes"]), abs(a["no"] - b["no"]))
            if diff >= 3.0:
                cross.append({
                    "question": a["question"][:100],
                    "platform_a": a["platform"], "yes_a": a["yes"], "no_a": a["no"], "url_a": a["url"],
                    "platform_b": b["platform"], "yes_b": b["yes"], "no_b": b["no"], "url_b": b["url"],
                    "diff": round(diff, 1), "volume": 0, "exact": False
                })
    return cross

def fmt_market(op):
    e = "🔥" if op["spread"] >= 3 else ("💡" if op["spread"] >= 1 else "🔎")
    vol = f"  📦 ${op['volume']:,.0f}" if op.get("volume", 0) > 100 else ""
    return (f"{e} *{op['platform']}*{vol}\n"
            f"❓ {op['question']}\n"
            f"✅ YES: {op['yes']}¢  ❌ NO: {op['no']}¢\n"
            f"💰 Спред: *+{op['spread']}%*\n"
            f"🔗 {op['url']}")

def fmt_cross(op):
    tag = "🎯" if op.get("exact") else "⚡"
    vol = f"  📦 ${op['volume']:,.0f}" if op.get("volume", 0) > 100 else ""
    return (f"{tag} *КРОСС +{op['diff']}%*{vol}\n"
            f"❓ {op['question']}\n\n"
            f"*{op['platform_a']}:* YES {op['yes_a']}¢ / NO {op['no_a']}¢\n"
            f"🔗 {op['url_a']}\n"
            f"*{op['platform_b']}:* YES {op['yes_b']}¢ / NO {op['no_b']}¢\n"
            f"🔗 {op['url_b']}")

def main_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔍 Сканировать", callback_data="scan")],
        [InlineKeyboardButton("📊 Статус", callback_data="status"),
         InlineKeyboardButton("⚙️ Настройки", callback_data="settings")],
        [InlineKeyboardButton("🎯 0.1%", callback_data="sp_0.1"),
         InlineKeyboardButton("🎯 1%", callback_data="sp_1"),
         InlineKeyboardButton("🎯 3%", callback_data="sp_3"),
         InlineKeyboardButton("🎯 5%", callback_data="sp_5")],
        [InlineKeyboardButton("📋 Топ 10", callback_data="lim_10"),
         InlineKeyboardButton("📋 Топ 20", callback_data="lim_20"),
         InlineKeyboardButton("📋 Топ 50", callback_data="lim_50")]
    ])

async def send_safe(bot, text, **kwargs):
    try:
        await bot.send_message(chat_id=CHAT_ID, text=text, **kwargs)
    except TimedOut:
        await asyncio.sleep(3)
        try:
            await bot.send_message(chat_id=CHAT_ID, text=text, **kwargs)
        except:
            pass
    except Exception as e:
        print(f"send error: {e}")

async def do_scan(bot):
    print("Сканирую...")
    funcs = [get_polymarket, get_polymarket_gamma, get_manifold, get_kalshi,
             get_predictit, get_limitless, get_opinion, get_predictfun]
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(f) for f in funcs]
        clob, gamma, mani, kals, pred, lim, opin, pf = [f.result() for f in futures]

    poly = merge_poly(clob, gamma)
    all_markets = poly + mani + kals + pred + lim + opin + pf
    print(f"Итого: {len(all_markets)}")

    inner = sorted([m for m in all_markets if m["spread"] >= MIN_SPREAD],
                   key=lambda x: x["spread"], reverse=True)
    cross_e = find_cross_cid(all_markets)
    cross_t = find_cross_text(all_markets)
    seen = set()
    cross = []
    for c in sorted(cross_e + cross_t, key=lambda x: x["diff"], reverse=True):
        k = f"{c['platform_a']}_{c['platform_b']}_{c['question'][:20]}"
        if k not in seen:
            seen.add(k)
            cross.append(c)

    total = len(inner) + len(cross)
    now = datetime.now().strftime("%H:%M:%S")

    if total == 0:
        await send_safe(bot,
            f"😴 *Арбитражей >{MIN_SPREAD}% не найдено*\n"
            f"🌐 Рынков: {len(all_markets)}\n🕐 {now}",
            parse_mode="Markdown", reply_markup=main_menu())
        return

    hot = len([x for x in inner if x['spread'] >= 3])
    med = len([x for x in inner if 1 <= x['spread'] < 3])
    low = len([x for x in inner if x['spread'] < 1])
    exact = len([x for x in cross if x.get("exact")])

    await send_safe(bot,
        f"📡 *Готово — {now}*\n\n"
        f"🌐 Рынков: *{len(all_markets)}*\n"
        f"  ├ Polymarket: {len(poly)}\n"
        f"  ├ Manifold: {len(mani)}\n"
        f"  ├ Kalshi: {len(kals)}\n"
        f"  ├ PredictIt: {len(pred)}\n"
        f"  ├ Limitless: {len(lim)}\n"
        f"  ├ Opinion: {len(opin)}\n"
        f"  └ PredictFun: {len(pf)}\n\n"
        f"💰 Найдено: *{total}*\n"
        f"  ├ 🔥 ≥3%: {hot}\n"
        f"  ├ 💡 1–3%: {med}\n"
        f"  ├ 🔎 <1%: {low}\n"
        f"  ├ 🎯 Точных кросс: {exact}\n"
        f"  └ ⚡ Кросс всего: {len(cross)}",
        parse_mode="Markdown", reply_markup=main_menu())

    await asyncio.sleep(0.5)

    if inner:
        await send_safe(bot, "━━━ 🏆 ТОП СПРЕДЫ ━━━")
        for op in inner[:MAX_RESULTS]:
            await send_safe(bot, fmt_market(op), parse_mode="Markdown", disable_web_page_preview=True)
            await asyncio.sleep(0.35)

    if cross:
        await send_safe(bot, "━━━ 🎯 КРОСС-АРБИТРАЖ ━━━")
        for op in cross[:MAX_RESULTS]:
            await send_safe(bot, fmt_cross(op), parse_mode="Markdown", disable_web_page_preview=True)
            await asyncio.sleep(0.35)

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 *Арбитраж-бот*\n\n"
        "🌐 Polymarket · Manifold · Kalshi\n"
        "   PredictIt · Limitless · Opinion · PredictFun\n\n"
        f"⏱ Авто-скан каждые {CHECK_INTERVAL // 60} мин",
        parse_mode="Markdown", reply_markup=main_menu())

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global MIN_SPREAD, MAX_RESULTS
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest:
        pass
    d = query.data
    try:
        if d == "scan":
            await query.edit_message_text("🔍 *Сканирую все платформы...*", parse_mode="Markdown")
            await do_scan(context.application.bot)
        elif d == "status":
            await query.edit_message_text(
                f"✅ *Работает*\n⏱ Каждые {CHECK_INTERVAL // 60} мин\n"
                f"📊 Мин. спред: {MIN_SPREAD}%\n📋 Макс: {MAX_RESULTS}",
                parse_mode="Markdown", reply_markup=main_menu())
        elif d == "settings":
            await query.edit_message_text(
                f"⚙️ *Настройки*\n📊 Спред: *{MIN_SPREAD}%*\n📋 Макс: *{MAX_RESULTS}*",
                parse_mode="Markdown", reply_markup=main_menu())
        elif d.startswith("sp_"):
            MIN_SPREAD = float(d.split("_")[1])
            await query.edit_message_text(
                f"✅ Мин. спред: *{MIN_SPREAD}%*",
                parse_mode="Markdown", reply_markup=main_menu())
        elif d.startswith("lim_"):
            MAX_RESULTS = int(d.split("_")[1])
            await query.edit_message_text(
                f"✅ Макс. результатов: *{MAX_RESULTS}*",
                parse_mode="Markdown", reply_markup=main_menu())
    except Exception as e:
        print(f"button error: {e}")

async def auto_check(context: ContextTypes.DEFAULT_TYPE):
    await do_scan(context.bot)

def main():
    print("Запуск бота...")
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.job_queue.run_repeating(auto_check, interval=CHECK_INTERVAL, first=20)
    print("Бот запущен!")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
