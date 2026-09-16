# -*- coding: utf-8 -*-
"""
================================================================================
乖离率策略 · 任务引擎 (tasks.py)
================================================================================
★ 设计原则：**任务只由用户决定是否执行**——
   · 「立即执行」= 用户点一下就跑
   · 「定时执行」= 用户在网页上显式开启后才生效（默认全部关闭）
   网站本身绝不偷偷定时。

三个基础任务：
  ① 盘前 premarket   —— 开盘前 15 分钟，明确「今天买什么、卖什么、还是什么都不做」
  ② 盘中 intraday    —— 盘中盯盘，只看持仓，预警止盈/止损是否触发
  ③ 盘后 postmarket  —— 收盘后跑全量数据，算出今日广度，决定「明天是否出手」
================================================================================
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time
from datetime import datetime, timedelta

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import strategy_core as core
import db

# ------------------------------------------------------------------ 任务定义
TASK_DEFS = {
    "premarket": dict(
        key="premarket", name="盘前任务", icon="🌅",
        suggest="09:10",
        desc="开盘前 15 分钟跑。告诉你今天开盘「买什么、卖什么、还是什么都不做」。",
        long_desc=("读取最近一个已收盘交易日的广度和候选股，结合你的持仓体检结果，"
                   "生成一张「今日操作清单」——需要卖出的持仓、需要买入的候选股，一条条列清楚。"
                   "不需要重新下载数据，几秒就跑完。"),
    ),
    "intraday": dict(
        key="intraday", name="盘中任务", icon="⏱",
        suggest="14:30",
        desc="盘中盯盘，只检查你的持仓，预警止盈/止损是否已被触发。",
        long_desc=("只拉取你持仓那几只股票的实时价格（很快），逐只算出浮盈浮亏，"
                   "并提示「已到 +6% 可卖一半」「已到 +10% 该清仓」「已跌破 -6% 预警」"
                   "「从最高点回撤 3% 该走」。注意：策略规则以收盘价为准，盘中只作预警。"),
    ),
    "postmarket": dict(
        key="postmarket", name="盘后任务", icon="🌙",
        suggest="15:35",
        desc="收盘后跑。更新全市场数据，算出今日广度，决定「明天是否出手」。",
        long_desc=("这是最核心的任务：拉取全市场 5017 只股票的最新日线（约 20~25 分钟），"
                   "算出今日信号广度。广度 ≥ 门槛 → 输出明天的买入候选股；"
                   "否则明确告诉你「明天不动手」。同时完成持仓体检和「明日操作计划」。"
                   "★ 数据源时效：新浪的「当日」日线通常要到当天傍晚才发布。"
                   "15:35 就跑的话，拿到的很可能仍是上一个交易日的数据，"
                   "页面会明确提示「数据源尚未发布 XX 行情」——那种情况傍晚再跑一次即可，"
                   "不用重复跑，重复跑不会更快拿到当天数据。"),
    ),
}
TASK_KEYS = ["premarket", "intraday", "postmarket"]

# 运行态（供网页轮询进度）
STATE: dict[str, dict] = {
    k: dict(running=False, phase="", done=0, total=0, msg="", started_at=None)
    for k in TASK_KEYS
}
_LOCK = threading.Lock()


# ------------------------------------------------------------------ 出场参数（读配置）
# ★ 修复：以前 0.94 / 1.06 / 1.10 / 1.05 / 0.97 / 10 这些数字是**直接写死**在代码里的，
#   导致用户按说明书改了 strategy_config.json 里的止损/止盈/持有上限后，
#   「持仓体检」和网页上的止损线、止盈线**根本不跟着变**（改了个寂寞）。
#   现在统一从配置读，改完刷新页面即生效，无需重启。
_EXIT_DEFAULTS = dict(
    hard_stop_loss_pct=-6.0,             # 收盘价跌破买入价 -6% → 卖出
    take_profit_tiers_pct=[6.0, 10.0],   # 分档止盈：+6% 卖一半 / +10% 清仓
    trailing_trigger_pct=5.0,            # 浮盈曾达 +5%
    trailing_drawdown_pct=3.0,           # 之后从最高点回撤 3% → 清仓
    max_hold_days=10,                    # 最长持有交易日
    max_daily_signals=10,                # 单日最多出手几只
)


def exit_config() -> dict:
    """出场规则参数。每次调用都重读配置文件，所以改完立刻生效。"""
    c = dict(_EXIT_DEFAULTS)
    try:
        with open(core.CONFIG_JSON, "r", encoding="utf-8") as f:
            j = json.load(f)
        if j.get("hard_stop_loss_pct") is not None:
            c["hard_stop_loss_pct"] = float(j["hard_stop_loss_pct"])
        tp = j.get("take_profit_tiers_pct")
        if isinstance(tp, (list, tuple)) and len(tp) >= 2:
            c["take_profit_tiers_pct"] = [float(tp[0]), float(tp[1])]
        ts = j.get("trailing_stop") or {}
        if ts.get("trigger_pct") is not None:
            c["trailing_trigger_pct"] = float(ts["trigger_pct"])
        if ts.get("drawdown_pct") is not None:
            c["trailing_drawdown_pct"] = float(ts["drawdown_pct"])
        if j.get("max_hold_days"):
            c["max_hold_days"] = int(j["max_hold_days"])
        mds = (j.get("position") or {}).get("max_daily_signals")
        if mds:
            c["max_daily_signals"] = int(mds)
    except Exception as e:
        print(f"[exit_config] 读取失败，使用默认值：{e}")
    return c


# ------------------------------------------------------------------ 持仓体检
def _holding_days(code: str, buy_date: str) -> int:
    """买入至今经过的交易天数（按该股本地日线计数）"""
    p = os.path.join(core.DATA_DIR, f"{str(code).zfill(6)}.csv")
    if not os.path.exists(p):
        return 0
    try:
        d = pd.read_csv(p, usecols=["date"])
        return int((d["date"] > str(buy_date)).sum())
    except Exception:
        return 0


def evaluate_holding(h: dict, price: float | None = None,
                     quote_date: str = "", latest_market_date: str = "",
                     intraday: bool = False) -> dict:
    """给一条持仓附加行情与卖出提示。

    price = None 时从本地缓存取最新收盘价；盘中任务传入实时价覆盖。
    intraday=True 时提示语用「预警」（盘中口径），否则用「确认」（收盘口径）。
    """
    out = dict(h)
    buy = float(h["buy_price"] or 0)
    ec = exit_config()
    out["max_hold_days"] = ec["max_hold_days"]

    if price is None:
        q = core.latest_quote(h["code"])
        if not q:
            out.update(last=None, pnl_pct=None, quote_date="", action="持有",
                       level="warn", days_held=_holding_days(h["code"], h["buy_date"] or ""),
                       alerts=["未找到本地行情，请先跑一次盘后任务"])
            return out
        price, quote_date = q["close"], q["date"]

    out["quote_date"] = quote_date or ""
    out["last"] = round(float(price), 3)
    out["pnl_pct"] = round((price - buy) / buy * 100, 2) if buy else None
    out["days_held"] = _holding_days(h["code"], h["buy_date"] or "")
    out["alerts"] = []
    out["action"] = "持有"
    out["level"] = "ok"

    # ★ 阈值全部来自 strategy_config.json（改配置立即生效）
    stop_mult = 1 + ec["hard_stop_loss_pct"] / 100.0
    tp1_mult = 1 + ec["take_profit_tiers_pct"][0] / 100.0
    tp2_mult = 1 + ec["take_profit_tiers_pct"][1] / 100.0
    trail_trigger = 1 + ec["trailing_trigger_pct"] / 100.0
    trail_dd = 1 - ec["trailing_drawdown_pct"] / 100.0

    stop = float(h.get("stop_price") or buy * stop_mult)
    tp1 = float(h.get("tp1_price") or buy * tp1_mult)
    tp2 = float(h.get("tp2_price") or buy * tp2_mult)
    peak = max(float(h.get("peak_price") or buy), float(price))

    # 刷新历史最高价（让「移动止盈」能跨天工作）
    if peak > float(h.get("peak_price") or buy) and h.get("id"):
        try:
            db.update_peak(h["id"], round(peak, 3))
        except Exception:
            pass

    out.update(stop_price=round(stop, 3), tp1_price=round(tp1, 3),
               tp2_price=round(tp2, 3), peak_price=round(peak, 3))

    tag = "预警" if intraday else "确认"
    stop_pct = ec["hard_stop_loss_pct"]
    tp1_pct, tp2_pct = ec["take_profit_tiers_pct"]

    if price < stop:
        out["action"] = "★ 卖出（跌破止损线）"
        out["level"] = "danger"
        out["alerts"].append(f"{tag}：{price:.2f} < 止损线 {stop:.2f}（{stop_pct:+.0f}%）")
    elif peak >= buy * trail_trigger and price <= peak * trail_dd:
        out["action"] = "★ 清仓（移动止盈回撤）"
        out["level"] = "danger"
        out["alerts"].append(
            f"{tag}：最高 {peak:.2f} 回撤到 {price:.2f}（≥{ec['trailing_drawdown_pct']:.0f}%）")
    elif price >= tp2:
        out["action"] = f"★ 全部清仓（+{tp2_pct:.0f}%）"
        out["level"] = "win"
        out["alerts"].append(f"{tag}：已达 +{tp2_pct:.0f}% 止盈线 {tp2:.2f}")
    elif price >= tp1:
        out["action"] = f"卖出一半（+{tp1_pct:.0f}%）"
        out["level"] = "win"
        out["alerts"].append(f"{tag}：已达 +{tp1_pct:.0f}% 止盈线 {tp1:.2f}")
    if out["days_held"] >= ec["max_hold_days"]:
        # ★ 到期只是「兜底卖出」条件，绝不能覆盖更紧急的止损 / 移动止盈 / 止盈信号。
        #   原实现无条件覆盖 action，会出现「明明已跌破止损线，却显示到期清仓」的误导。
        if out["action"] == "持有":
            out["action"] = f"★ 到期清仓（满 {ec['max_hold_days']} 个交易日）"
        if out["level"] == "ok":
            out["level"] = "warn"
        out["alerts"].append(f"已持有 {out['days_held']} 个交易日（上限 {ec['max_hold_days']} 日）")

    # 行情非最新提示（盘中不提示，因为盘中本来就是当日实时价）
    if (not intraday) and latest_market_date and str(quote_date) < str(latest_market_date):
        out["alerts"].append(f"行情为 {quote_date}，非最新")
        if out["level"] == "ok":
            out["level"] = "warn"
    return out


def evaluate_holdings(quotes: dict | None = None, quote_date: str = "",
                      latest_market_date: str = "", intraday: bool = False) -> list[dict]:
    """体检全部在持持仓。quotes={code: price} 可覆盖最新价（盘中用）。"""
    res = []
    for h in db.list_holdings("holding"):
        p = quotes.get(h["code"]) if quotes else None
        res.append(evaluate_holding(h, price=p, quote_date=quote_date,
                                    latest_market_date=latest_market_date,
                                    intraday=intraday))
    return res


def _need_action(hs: list[dict]) -> list[dict]:
    """筛出需要今天动手的持仓"""
    return [h for h in hs if h["level"] in ("danger", "win", "warn")
            or h["action"] != "持有"]


# ------------------------------------------------------------------ 公共取数
def _latest_daily() -> dict | None:
    return db.get_daily()


def _market_date() -> str:
    d = db.get_daily()
    return d["date"] if d else ""


def _cand_rows(date: str) -> list[dict]:
    return db.get_candidates(date)


# ================================================================== ① 盘前任务
def run_premarket(log=None) -> dict:
    log = log or (lambda s: print(s, flush=True))
    log("[盘前] 读取最近一个已收盘交易日的数据…")

    d = _latest_daily()
    if not d:
        return dict(headline="还没有任何数据，请先跑一次「盘后任务」。",
                    level="warn",
                    blocks=[dict(title="怎么办", kind="text",
                                 text="打开工具站 → 任务中心 → 点击「盘后任务」的「立即执行」。"
                                      "第一次需要约 20~25 分钟。")])

    th = d["threshold"]
    trig = bool(d["triggered"])
    log(f"[盘前] 最近交易日 {d['date']}：广度 {d['breadth']} / 门槛 {th}")

    # 持仓体检（用最近收盘价）
    hs = evaluate_holdings(latest_market_date=d["date"])
    act = _need_action(hs)

    blocks = []
    total = len(hs)

    # ---- 结论 ----
    if act:
        headline = f"今天有 {len(act)} 项持仓动作要处理（见下方清单）。"
        level = "danger" if any(x["level"] == "danger" for x in act) else "warn"
    elif trig:
        headline = "今天开盘可以按计划买入昨日候选股。"
        level = "ok"
    else:
        headline = "今天不买也不卖，继续空仓等待。"
        level = "neutral"

    # ---- 待卖清单 ----
    if hs:
        blocks.append(dict(
            title=f"持仓体检（共 {total} 只，需处理 {len(act)} 只）",
            kind="holdings",
            rows=act if act else hs))
    else:
        blocks.append(dict(title="持仓体检", kind="text", text="当前没有持仓记录。"))

    # ---- 待买清单 ----
    if trig:
        cands = _cand_rows(d["date"])
        if cands:
            blocks.append(dict(
                title=f"今日开盘买入清单（{d['date']} 选出的候选股，共 {len(cands)} 只）",
                kind="candidates", rows=cands))
            ec_ = exit_config()
            blocks.append(dict(
                title="怎么买",
                kind="text",
                text=("· 开盘后分 2~3 批买入，不要一次全仓\n"
                      f"· 最多 {len(cands)} 只，资金平均分配\n"
                      f"· 买入价即你的成本，止损线 = 买入价 × "
                      f"{1 + ec_['hard_stop_loss_pct'] / 100:.2f}"
                      f"（{ec_['hard_stop_loss_pct']:+.0f}%）")))
        else:
            blocks.append(dict(title="今日买入清单", kind="text",
                               text="昨日广度达标但未选出个股（极少见）。"))
    else:
        blocks.append(dict(
            title="今日买入清单：无",
            kind="text",
            text=(f"最近交易日 {d['date']} 的信号广度只有 {d['breadth']} 只，"
                  f"未达到出手门槛 {th} 只。\n"
                  "★ 空仓等待本身就是这个策略的一部分——六年里只有 22 天达标。")))

    # ---- 数据新鲜度 ----
    fresh = datetime.now().strftime("%Y-%m-%d")
    if d["date"] != fresh:
        gap = ""
        try:
            gap = f"（距今 {(pd.Timestamp(fresh) - pd.Timestamp(d['date'])).days} 天）"
        except Exception:
            pass
        blocks.append(dict(
            title="数据提醒", kind="text",
            text=f"当前采用的是 {d['date']} 的收盘数据{gap}。\n"
                 "如果昨天收盘后没有跑「盘后任务」，数据可能已过期。"))

    detail = _render_detail(headline, blocks)
    log(f"[盘前] {headline}")
    return dict(headline=headline, level=level, blocks=blocks, detail=detail,
                market_date=d["date"], breadth=d["breadth"], threshold=th,
                triggered=trig)


# ================================================================== ② 盘中任务
def run_intraday(log=None) -> dict:
    log = log or (lambda s: print(s, flush=True))
    hs = db.list_holdings("holding")

    if not hs:
        ec_ = exit_config()
        t1, t2 = ec_["take_profit_tiers_pct"]
        msg = ("当前没有持仓，盘中无需盯盘。\n\n"
               "盘中任务的用途：当你持有股票时，实时监控它们是否触及"
               f"止盈（+{t1:.0f}% / +{t2:.0f}%）、止损（{ec_['hard_stop_loss_pct']:+.0f}%）"
               f"或移动止盈（回撤 {ec_['trailing_drawdown_pct']:.0f}%）。")
        log("[盘中] 无持仓，跳过")
        return dict(headline="当前没有持仓，盘中无需盯盘。", level="neutral",
                    blocks=[dict(title="说明", kind="text", text=msg)],
                    detail=msg)

    log(f"[盘中] 拉取 {len(hs)} 只持仓的实时行情…")
    codes = [str(h["code"]).zfill(6) for h in hs]
    rt = core.fetch_live_quotes(codes)

    quotes, fails, qdate, src = {}, [], "", ""
    for h in hs:
        q = rt.get(str(h["code"]).zfill(6))
        if q and q.get("close"):
            quotes[h["code"]] = float(q["close"])
            qdate = q.get("date") or qdate
            src = q.get("source") or src
            log(f"    {h['code']} {q.get('name') or h.get('name') or ''} → "
                f"{q['close']:.2f}（{q.get('date')} {q.get('time')} · {q.get('source')}）")
        else:
            fails.append(h["code"])
            log(f"    {h['code']} 实时行情获取失败，回退本地缓存")

    today = datetime.now().strftime("%Y-%m-%d")
    ev = evaluate_holdings(quotes=quotes, quote_date=qdate or today,
                           latest_market_date=today, intraday=True)
    act = _need_action(ev)

    if act:
        danger = [x for x in act if x["level"] == "danger"]
        headline = (f"⚠️ 盘中预警：{len(act)} 只持仓触发了卖出条件"
                    if danger else f"盘中提示：{len(act)} 只持仓可考虑操作")
        level = "danger" if danger else "warn"
    else:
        headline = f"{len(ev)} 只持仓均未触发条件，继续持有。"
        level = "ok"

    blocks = [dict(title=f"持仓实时体检（{len(ev)} 只，触发 {len(act)} 只）",
                   kind="holdings", rows=act if act else ev)]
    if src:
        blocks.append(dict(title=f"行情来源：{src}"
                                 + (f"（{qdate} 实时）" if qdate else ""),
                           kind="text",
                           text=("盘中价格来自腾讯/新浪实时快照接口。\n"
                                 "注意：本地缓存的日线数据要等收盘后跑「盘后任务」才会更新。")))
    ec_ = exit_config()
    t1, t2 = ec_["take_profit_tiers_pct"]
    blocks.append(dict(
        title="重要说明", kind="text",
        text=("· 本策略的止损/止盈**以收盘价为准**，盘中只作预警，不要因为盘中插针就慌。\n"
              "· 止损执行时点 = 收盘确认跌破后，**次日开盘卖出**。\n"
              f"· 若盘中已到 +{t1:.0f}% / +{t2:.0f}%，可以考虑提前落袋，但不是必须。")))
    if fails:
        blocks.append(dict(title="行情获取失败", kind="text",
                           text="以下股票未取到实时行情，已回退用最近收盘价：" + "、".join(fails)))

    detail = _render_detail(headline, blocks)
    log(f"[盘中] {headline}")
    return dict(headline=headline, level=level, blocks=blocks, detail=detail,
                market_date=today, quote_date=qdate)


# ================================================================== ③ 盘后任务
def run_postmarket(log=None, limit: int = 0, no_fetch: bool = False) -> dict:
    log = log or (lambda s: print(s, flush=True))
    key = "postmarket"

    def prog(done, total, ok, fail):
        with _LOCK:
            STATE[key].update(phase="下载全市场数据", done=done, total=total,
                              msg=f"{done}/{total}（成功 {ok} 失败 {fail}）")

    log("[盘后] 开始更新全市场数据…")
    res = _run_update_with_progress(log, limit=limit, no_fetch=no_fetch, progress=prog)

    with _LOCK:
        STATE[key].update(phase="体检持仓", done=0, total=0, msg="")

    hs = evaluate_holdings(latest_market_date=res["date"])
    act = _need_action(hs)

    trig = bool(res["triggered"])
    blocks = []

    if trig:
        headline = f"{res['date']} 广度 {res['breadth']} 达标 → 明天可以出手！"
        level = "ok"
        ec_ = exit_config()
        t1, t2 = ec_["take_profit_tiers_pct"]
        blocks.append(dict(
            title=f"★ 明日买入候选股（{len(res['candidates'])} 只）",
            kind="candidates", rows=res["candidates"]))
        blocks.append(dict(
            title="明日怎么操作", kind="text",
            text=("1. 明天开盘后分 2~3 批买入这些股票，资金平均分配\n"
                  f"2. 最多 {ec_.get('max_daily_signals', 10)} 只，单只不超过总资金的 1/10~1/5\n"
                  "3. 买入后立刻到工具站「我的持仓」登记，页面会自动帮你盯止损止盈\n"
                  f"4. 止损线 = 买入价 × {1 + ec_['hard_stop_loss_pct'] / 100:.2f}"
                  f"（{ec_['hard_stop_loss_pct']:+.0f}%）；止盈 +{t1:.0f}% 卖一半、"
                  f"+{t2:.0f}% 清仓；最长持有 {ec_['max_hold_days']} 个交易日")))
    else:
        headline = f"{res['date']} 广度 {res['breadth']} 未达标 → 明天不动手。"
        level = "neutral"
        blocks.append(dict(
            title="为什么不动手", kind="text",
            text=(f"今日信号广度 {res['breadth']} 只，未达到出手门槛 {res['threshold']} 只。\n"
                  f"（仅乖离率≤{core.CFG['bias_threshold']}% 的股票有 {res['bias_only']} 只，"
                  "但没同时满足放量/成交额等条件。）\n\n"
                  "★ 记住：机会极稀疏是这个策略的常态，六年里只有 22 天达标。")))
        blocks.append(dict(
            title="今日数据概览", kind="kv", rows=[
                dict(k="交易日", v=res["date"]),
                dict(k="信号广度", v=f"{res['breadth']} 只"),
                dict(k="出手门槛", v=f"≥ {res['threshold']} 只"),
                dict(k="仅乖离率命中", v=f"{res['bias_only']} 只"),
                dict(k="全市场股票", v=f"{res['stocks_total']} 只"),
                dict(k="下载成功/失败", v=f"{res['fetch_ok']} / {res['fetch_fail']}"),
                dict(k="耗时", v=f"{res['elapsed_min']} 分钟"),
            ]))

    if act:
        blocks.append(dict(title=f"持仓体检（需处理 {len(act)} 只）",
                           kind="holdings", rows=act))
    elif hs:
        blocks.append(dict(title=f"持仓体检（{len(hs)} 只，均正常持有）",
                           kind="holdings", rows=hs))

    detail = _render_detail(headline, blocks)
    log(f"[盘后] {headline}")
    res2 = dict(headline=headline, level=level, blocks=blocks, detail=detail)
    res2.update(market_date=res["date"], breadth=res["breadth"],
                threshold=res["threshold"], triggered=trig)
    return res2


def _run_update_with_progress(log, limit=0, no_fetch=False, progress=None):
    """调用 updater.run_update，并把日志同时写进 STATE"""
    import updater
    key = "postmarket"

    def lg(s):
        with _LOCK:
            STATE[key]["msg"] = str(s)
        log(s)

    return updater.run_update(limit=limit, no_fetch=no_fetch, log=lg, progress=progress)


# ================================================================== 调度执行
RUNNERS = {
    "premarket": run_premarket,
    "intraday": run_intraday,
    "postmarket": run_postmarket,
}


def run_task(key: str, trigger_by: str = "manual", log=None,
             reserved: bool = False) -> dict:
    """执行一个任务并记录到数据库。返回结果 dict。

    reserved=True 表示调用方（run_task_async）已经**原子地**占好了 STATE 里的
    running 位，此时不再重复检查，避免"检查—启动"之间的竞态。
    """
    if key not in RUNNERS:
        raise ValueError(f"未知任务：{key}")
    log = log or (lambda s: print(s, flush=True))

    with _LOCK:
        if STATE[key]["running"] and not reserved:
            return dict(headline="该任务正在执行中，请稍候…", level="warn",
                        blocks=[dict(title="提示", kind="text",
                                     text="同一个任务不能重复执行。")])
        STATE[key].update(running=True, phase="启动", done=0, total=0,
                          msg="任务开始…", started_at=datetime.now().isoformat(timespec="seconds"))

    # ★ 跨进程互斥：控制台进程与网页进程各自的内存状态互不可见，
    #   若两边同时跑同一个任务，会同时写同一批 CSV / daily 行 → 数据竞争。
    #   这里查数据库里的 running 记录来兜住这种情况（has_running_task 会自动
    #   清理超过 2 小时的僵尸记录，避免进程被强杀后永久占位）。
    try:
        if key in db.has_running_task():
            with _LOCK:
                STATE[key].update(running=False, phase="", done=0, total=0, msg="")
            return dict(headline="该任务正在执行中（可能由控制台或另一个窗口触发），请稍候…",
                        level="warn",
                        blocks=[dict(title="提示", kind="text",
                                     text="同一个任务不能同时执行两次，请等它跑完。")])
    except Exception:
        pass

    run_id = db.start_task_run(key, trigger_by)
    t0 = time.time()
    try:
        result = RUNNERS[key](log=log)
        title = result.get("headline", "执行完成")
        db.finish_task_run(run_id, "ok", title,
                           summary=json.dumps(result, ensure_ascii=False, default=str),
                           detail=result.get("detail", ""))
        result["status"] = "ok"
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        log(f"[错误] {key} 执行失败：{e}")
        log(tb)
        db.finish_task_run(run_id, "fail", f"执行失败：{e}", summary="", detail=tb)
        result = dict(headline=f"执行失败：{e}", level="danger",
                      blocks=[dict(title="错误详情", kind="text", text=tb)],
                      detail=tb, status="fail")
    finally:
        with _LOCK:
            STATE[key].update(running=False, phase="完成", msg="", 
                              done=0, total=0)
    result["elapsed_sec"] = round(time.time() - t0, 1)
    result["run_id"] = run_id
    return result


def run_task_async(key: str, trigger_by: str = "manual") -> bool:
    """后台线程执行（供网页 API 调用）。

    ★ 必须在同一把锁内「检查 + 占位」，否则两个几乎同时到达的请求
      （例如双击两次「立即执行」）会同时通过检查，把同一个任务跑两遍。
    """
    with _LOCK:
        if STATE[key]["running"]:
            return False
        STATE[key]["running"] = True          # ★ 原子占位
    try:
        threading.Thread(target=run_task, args=(key, trigger_by),
                         kwargs=dict(reserved=True), daemon=True).start()
    except Exception:
        with _LOCK:
            STATE[key]["running"] = False     # 起线程失败要回滚，别把任务永久锁死
        raise
    return True


# ------------------------------------------------------------------ 辅助
def _render_detail(headline: str, blocks: list[dict]) -> str:
    """把结构化结果渲染成纯文本（给控制台/日志用）"""
    lines = [headline, ""]
    for b in blocks:
        lines.append(f"── {b.get('title','')}")
        kind = b.get("kind")
        if kind == "text":
            lines.append(b.get("text", ""))
        elif kind == "kv":
            for r in b.get("rows", []):
                lines.append(f"   {r.get('k')}：{r.get('v')}")
        elif kind == "holdings":
            for r in b.get("rows", []):
                lines.append(
                    f"   {r.get('code')} {r.get('name') or '':<6} "
                    f"买 {r.get('buy_price')} 现 {r.get('last')} "
                    f"{('%+.2f%%' % r['pnl_pct']) if r.get('pnl_pct') is not None else ''} "
                    f"持有{r.get('days_held')}日 → {r.get('action')}")
                for a in r.get("alerts", []):
                    lines.append(f"        · {a}")
        elif kind == "candidates":
            for i, r in enumerate(b.get("rows", []), 1):
                lines.append(
                    f"   {i:>2}. {r.get('code')} {r.get('name') or '':<6} "
                    f"现价 {r.get('close')}  BIAS {r.get('bias'):.2f}%  "
                    f"换手 {r.get('turnover_pct')}%  量比 {r.get('vol_ratio')}")
        lines.append("")
    return "\n".join(lines)


def get_task_overview() -> list[dict]:
    """给网页用：任务定义 + 定时设置 + 最近执行 + 运行态"""
    settings = db.get_task_settings()
    out = []
    for k in TASK_KEYS:
        d = dict(TASK_DEFS[k])
        st = settings.get(k, {})
        last = db.get_last_run(k)
        d.update(
            enabled=bool(st.get("enabled", 0)),
            at_time=st.get("at_time") or TASK_DEFS[k]["suggest"],
            days=st.get("days") or "1,2,3,4,5",
            running=STATE[k]["running"],
            phase=STATE[k]["phase"],
            progress=dict(done=STATE[k]["done"], total=STATE[k]["total"],
                          msg=STATE[k]["msg"]),
            last_run=(dict(
                id=last["id"], status=last["status"], title=last["title"],
                started_at=last["started_at"], finished_at=last["finished_at"],
                trigger_by=last["trigger_by"]) if last else None),
        )
        out.append(d)
    return out
