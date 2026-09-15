# -*- coding: utf-8 -*-
"""
================================================================================
乖离率策略 · 核心引擎 (strategy_core.py)
================================================================================
职责：本地全市场数据的获取 / 缓存 / 指标计算 / 广度与候选股计算

口径说明（与六年回测逐条对应，因此本机算出的「广度 50」直接等于回测胜率 85.6%）：
    广度 = 当日全市场满足以下全部条件的股票数
        ① BIAS_24 <= -15%      （24日均线乖离率，前复权）
        ② 成交量 >= 5日均量 × 1.5
        ③ 近5日涨跌幅 < 0
        ④ 成交额 >= 8000万元
        ⑤ 上市 >= 60 个交易日
        ⑥ 非 ST / 非退市 / 非北交所

说明：回测另有「持有窗口内停牌防护」，属未来信息，实盘不可知，
      且仅影响全市场约 1.2% 的样本，故实盘口径不包含该条。
================================================================================
"""
from __future__ import annotations

import os
import sys
import json
import time
import socket
import threading
import glob
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutTimeout

import numpy as np
import pandas as pd

# ★ 关键修复：akshare 内部所有 requests.get() 都没有传 timeout，个别请求会永久挂死。
#   实测全市场 5017 只中有 8 只挂死 → as_completed 永远等不到结束 → 每日自动更新卡死。
#   设置全局 socket 默认超时后，requests/urllib3 会继承它，挂死请求最多等 20 秒即抛异常，
#   再配合 fetch_one 的重试逻辑，单只最坏 ~60 秒必定返回。
socket.setdefaulttimeout(20)

# ------------------------------------------------------------------ 路径
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # 股票工具/
WEBAPP = os.path.join(BASE, "webapp")
DATA_DIR = os.path.join(BASE, "data_long")
UNIVERSE_CSV = os.path.join(WEBAPP, "universe.csv")
# ★ 回测口径清单（5017 只，含 code/name）——必须优先使用，保证「广度 50」与回测语义一致
BACKTEST_UNIVERSE = os.path.join(DATA_DIR, "_universe.csv")
CONFIG_JSON = os.path.join(BASE, "strategy_config.json")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(WEBAPP, exist_ok=True)

# ------------------------------------------------------------------ 参数
DEFAULTS = dict(
    bias_period=24,          # 乖离率周期
    bias_threshold=-15.0,    # 信号阈值
    vol_surge=1.5,           # 放量倍数
    min_amount=8e7,          # 成交额下限（元）= 8000万
    min_list_days=60,        # 最少上市交易日
    breadth_threshold=50,    # ★ 出手门槛
    workers=16,              # 并发线程
    retry=3,
)


def load_config() -> dict:
    """从 strategy_config.json 读取参数，读不到就用默认值

    注意：成交额门槛 min_amount 不从配置里取——配置中的 min_total_mv_yuan 是
    「市值」门槛（50亿），与回测用的「成交额」门槛（8000万）是两个不同的概念。
    为保证本机算出的广度与六年回测口径完全一致，这里保持 8000万 不变。
    """
    cfg = dict(DEFAULTS)
    try:
        with open(CONFIG_JSON, "r", encoding="utf-8") as f:
            j = json.load(f)
        sig = j.get("signal", {})
        if "bias_period" in sig:
            cfg["bias_period"] = int(sig["bias_period"])
        if "bias_threshold_default_pct" in sig:
            cfg["bias_threshold"] = float(sig["bias_threshold_default_pct"])
        if "volume_ratio_min" in sig:
            cfg["vol_surge"] = float(sig["volume_ratio_min"])
        rg = j.get("regime_gate", {})
        key = rg.get("active_preset", "main")
        th = rg.get("presets", {}).get(key, {}).get("threshold")
        if th:
            cfg["breadth_threshold"] = int(th)
    except Exception as e:
        print(f"[config] 读取失败，使用默认值：{e}")
    return cfg


CFG = load_config()


# ------------------------------------------------------------------ 工具
def to_sym(code: str) -> str | None:
    """纯数字代码 → 带交易所前缀的代码（新浪接口要求）"""
    code = str(code).zfill(6)
    if code.startswith(("6", "9")):
        return "sh" + code
    if code.startswith(("0", "2", "3")):
        return "sz" + code
    return None  # 北交所等，忽略


def _patch_mini_racer():
    """新浪接口内部每次新建 V8 引擎，多线程并发构造会原生崩溃，这里串行化"""
    try:
        import py_mini_racer
    except Exception:
        return
    if getattr(py_mini_racer.MiniRacer, "_wb_serialized", False):
        return
    lock = threading.Lock()
    orig = py_mini_racer.MiniRacer.__init__

    def _init(self, *a, **k):
        with lock:
            orig(self, *a, **k)

    py_mini_racer.MiniRacer.__init__ = _init
    py_mini_racer.MiniRacer._wb_serialized = True


_patch_mini_racer()


# ------------------------------------------------------------------ 股票清单
def _normalize_universe(df: pd.DataFrame) -> pd.DataFrame:
    """统一成 code / name 两列，code 为 6 位字符串"""
    cols = list(df.columns)
    ccode = next((c for c in cols if str(c).lower() == "code" or "代码" in str(c)), None)
    cname = next((c for c in cols if str(c).lower() == "name" or "简称" in str(c) or "名称" in str(c)), None)
    if ccode is None:
        raise ValueError("清单缺少代码列")
    out = pd.DataFrame({"code": df[ccode].astype(str).str.zfill(6)})
    out["name"] = df[cname].astype(str) if cname is not None else ""
    return out


def load_universe(force: bool = False) -> pd.DataFrame:
    """获取沪深 A 股清单（含名称）。

    优先级：
      ① data_long/_universe.csv  —— 回测口径清单（5017 只），保证广度语义一致
      ② webapp/universe.csv      —— 本工具自己的缓存
      ③ 交易所官网实时拉取（兜底）
    """
    # ① 回测口径清单（首选，固定不变）
    if os.path.exists(BACKTEST_UNIVERSE) and os.path.getsize(BACKTEST_UNIVERSE) > 500:
        df = _normalize_universe(pd.read_csv(BACKTEST_UNIVERSE, dtype=str))
        df = df.drop_duplicates("code").reset_index(drop=True)
        return df

    # ② 本工具缓存
    if not force and os.path.exists(UNIVERSE_CSV) and os.path.getsize(UNIVERSE_CSV) > 500:
        df = _normalize_universe(pd.read_csv(UNIVERSE_CSV, dtype=str))
        return df.drop_duplicates("code").reset_index(drop=True)

    # ③ 交易所官网兜底
    import akshare as ak
    frames = []
    for fn, label in ((ak.stock_info_sh_name_code, "上交所"),
                      (ak.stock_info_sz_name_code, "深交所")):
        try:
            d = fn()
        except Exception as e:
            print(f"[universe] {label} 获取失败：{type(e).__name__}: {e}")
            continue
        cols = list(d.columns)
        ccode = next((c for c in cols if "代码" in str(c)), None)
        cname = next((c for c in cols if "简称" in str(c)), None)
        if ccode is None or cname is None:
            continue
        frames.append(pd.DataFrame({"code": d[ccode].astype(str), "name": d[cname].astype(str)}))

    if not frames:
        raise RuntimeError("股票清单获取失败（所有源不可用）")

    df = pd.concat(frames, ignore_index=True).drop_duplicates("code")
    n0 = len(df)
    df = df[~df["name"].str.contains("ST", na=False)]
    df = df[~df["name"].str.contains("退", na=False)]
    df["code"] = df["code"].str.zfill(6)
    df = df[~df["code"].str.startswith(("4", "8", "92"))]
    df = df.reset_index(drop=True)
    df.to_csv(UNIVERSE_CSV, index=False, encoding="utf-8-sig")
    print(f"[universe] {n0} → {len(df)} 只（已剔除 ST/退市/北交所）")
    return df


def load_names() -> dict:
    """返回 code → name 映射（用于展示候选股名称）"""
    try:
        u = load_universe()
        return dict(zip(u["code"], u["name"]))
    except Exception as e:
        print(f"[names] 名称映射获取失败：{e}")
        return {}


# ------------------------------------------------------------------ 数据获取
def fetch_one(code: str) -> pd.DataFrame | None:
    """拉取单只股票的前复权日线（最多重试 CFG['retry'] 次，失败返回 None）"""
    import akshare as ak
    sym = to_sym(code)
    if sym is None:
        return None
    for _ in range(CFG["retry"]):
        try:
            d = ak.stock_zh_a_daily(symbol=sym, adjust="qfq")
            if d is None or d.empty or "close" not in d.columns:
                return None
            d = d.copy()
            d["code"] = str(code).zfill(6)
            keep = [c for c in ["date", "open", "high", "low", "close", "volume", "amount", "turnover", "code"]
                    if c in d.columns]
            return d[keep]
        except Exception:
            time.sleep(0.4)
    return None


def update_cache(codes: list[str], workers: int | None = None, verbose: bool = True,
                 deadline_min: float = 45.0, progress=None) -> tuple[int, int]:
    """并发更新本地日线缓存。返回 (成功数, 失败数)

    deadline_min: 抓取阶段的时间上限（分钟）。超时即放弃剩余任务，
                  确保每日自动更新在任何网络异常下都能结束。
    progress: 可选回调 progress(done, total, ok, fail)，用于把进度推给网页。
    """
    workers = workers or CFG["workers"]
    ok = fail = 0
    t0 = time.time()
    total = len(codes)
    ex = ThreadPoolExecutor(max_workers=workers)
    try:
        futs = {ex.submit(job_one, c): c for c in codes}
        remain = max(t0 + deadline_min * 60 - time.time(), 1.0)
        try:
            for i, fu in enumerate(as_completed(futs, timeout=remain), 1):
                c, n = fu.result()
                if n:
                    ok += 1
                else:
                    fail += 1
                if progress is not None:
                    try:
                        progress(i, total, ok, fail)
                    except Exception:
                        pass
                if verbose and (i % 200 == 0 or i == total):
                    el = time.time() - t0
                    print(f"  更新进度 {i}/{total}  成功 {ok} 失败 {fail}  已用 {el/60:.1f} 分钟 "
                          f"（预计还需 {(el/i)*(total-i)/60:.1f} 分钟）", flush=True)
        except FutTimeout:
            left = total - ok - fail
            print(f"  ⚠️ 抓取超时（>{deadline_min:.0f} 分钟），放弃剩余 {left} 只："
                  f"成功 {ok} 失败 {fail}", flush=True)
            fail += left
    finally:
        ex.shutdown(wait=False, cancel_futures=True)   # 不等待挂死线程
    return ok, fail


def job_one(code: str):
    """单只抓取 + 落盘（update_cache 的工作单元）"""
    d = fetch_one(code)
    if d is None:
        return code, None
    p = os.path.join(DATA_DIR, f"{code}.csv")
    d.to_csv(p, index=False, encoding="utf-8-sig")
    return code, len(d)


def load_dataset(codes: list[str] | None = None, verbose: bool = True) -> pd.DataFrame:
    """从本地缓存汇总成一张大表。

    注意：本地缓存文件（data_long/{code}.csv）只有行情列，没有 code / name 列，
    因此这里从文件名解析 code 注入，保证后续 groupby("code") 可用。
    """
    files = [f for f in glob.glob(os.path.join(DATA_DIR, "*.csv"))
             if not os.path.basename(f).startswith("_")]
    if codes is not None:
        want = set(codes)
        files = [f for f in files if os.path.basename(f)[:-4] in want]

    frames = []
    for f in files:
        code = os.path.basename(f)[:-4]          # 去掉 .csv → 6 位代码
        try:
            d = pd.read_csv(f)
            if d.empty or "close" not in d.columns:
                continue
            if "code" not in d.columns:
                d["code"] = code
            else:
                d["code"] = d["code"].astype(str).str.zfill(6)
            frames.append(d)
        except Exception:
            continue
    if not frames:
        raise RuntimeError("没有可用的本地数据，请先更新数据")

    data = pd.concat(frames, ignore_index=True)
    data["date"] = pd.to_datetime(data["date"], errors="coerce")
    data = data.dropna(subset=["date"])
    for c in ["open", "close", "high", "low", "volume", "amount", "turnover"]:
        if c in data.columns:
            data[c] = pd.to_numeric(data[c], errors="coerce")
    data = data.dropna(subset=["open", "close", "high", "low"])
    data = data.sort_values(["code", "date"]).reset_index(drop=True)
    if verbose:
        print(f"[dataset] {len(data):,} 行 / {data['code'].nunique():,} 只 / "
              f"{data['date'].min():%Y-%m-%d} ~ {data['date'].max():%Y-%m-%d}")
    return data


# ------------------------------------------------------------------ 指标
def compute_features(data: pd.DataFrame) -> pd.DataFrame:
    """计算 BIAS / 5日均量 / 5日涨幅 / 放量标记"""
    g = data.groupby("code", sort=False)
    n = CFG["bias_period"]

    data["ma"] = g["close"].transform(lambda s: s.rolling(n, min_periods=n).mean())
    data["bias"] = (data["close"] - data["ma"]) / data["ma"] * 100
    data["vol_ma5"] = g["volume"].transform(lambda s: s.rolling(5, min_periods=3).mean())
    data["chg5d"] = g["close"].transform(lambda s: s.pct_change(5) * 100)
    data["bar_no"] = g.cumcount()          # 已上市交易日数
    data["vol_ratio"] = data["volume"] / data["vol_ma5"]

    data["vol_ok"] = data["volume"] >= data["vol_ma5"] * CFG["vol_surge"]
    data["chg5d_ok"] = data["chg5d"] < 0
    data["size_ok"] = data["bar_no"] >= CFG["min_list_days"]
    # ★ 成交额条件：本地缓存理论上一定有 amount 列；若无（数据源变动 / 老缓存），
    #   直接 data["amount"] 会抛 KeyError 让整次更新崩掉，这里做兜底并明确告警。
    if "amount" in data.columns:
        data["amt_ok"] = pd.to_numeric(data["amount"], errors="coerce") >= CFG["min_amount"]
    else:
        data["amt_ok"] = True
        print("[features] ⚠️ 数据缺少 amount 列，成交额门槛已跳过（广度会偏高）")
    if "turnover" in data.columns:
        data["turnover_pct"] = pd.to_numeric(data["turnover"], errors="coerce") * 100
    else:
        data["turnover_pct"] = np.nan
    return data


def signal_mask(d: pd.DataFrame) -> pd.Series:
    """基线信号条件（用于算广度）"""
    return (d["bias"] <= CFG["bias_threshold"]) & d["vol_ok"] & d["chg5d_ok"] \
        & d["amt_ok"] & d["size_ok"]


# ------------------------------------------------------------------ 广度 / 候选股
def latest_trade_date(data: pd.DataFrame) -> pd.Timestamp:
    return data["date"].max()


def calc_breadth(data: pd.DataFrame, date: pd.Timestamp | None = None) -> dict:
    """计算指定交易日的信号广度"""
    date = date or latest_trade_date(data)
    d = data[data["date"] == date]
    sig = d[signal_mask(d)]
    all_sig = d[d["bias"] <= CFG["bias_threshold"]]           # 仅乖离率条件，做参考
    th = CFG["breadth_threshold"]
    return dict(
        date=str(pd.Timestamp(date).date()),
        breadth=int(len(sig)),
        bias_only=int(len(all_sig)),
        threshold=th,
        triggered=bool(len(sig) >= th),
        stocks_total=int(len(d)),
    )


def pick_candidates(data: pd.DataFrame, date: pd.Timestamp | None = None, limit: int = 10) -> pd.DataFrame:
    """按 BIAS 升序取候选股（跌得最狠的在前），并补上股票名称"""
    date = date or latest_trade_date(data)
    d = data[data["date"] == date]
    sig = d[signal_mask(d)].copy()
    if sig.empty:
        return sig
    sig = sig.sort_values("bias").head(limit)
    sig["name"] = sig["code"].map(load_names())
    cols = ["code", "name", "close", "bias", "turnover_pct", "vol_ratio", "chg5d", "amount"]
    sig = sig[[c for c in cols if c in sig.columns]]
    return sig.reset_index(drop=True)


def breadth_history(data: pd.DataFrame, days: int = 250) -> pd.DataFrame:
    """历史广度序列（用于画曲线）：每个交易日的广度 + 仅乖离率命中数"""
    m = signal_mask(data)
    b_only = data["bias"] <= CFG["bias_threshold"]
    hist = data.groupby("date").size().rename("total").to_frame()
    br = data[m].groupby("date").size().rename("breadth")
    bo = data[b_only].groupby("date").size().rename("bias_only")
    out = hist.join(br, how="left").join(bo, how="left").fillna(0)
    out["breadth"] = out["breadth"].astype(int)
    out["bias_only"] = out["bias_only"].astype(int)
    return out.tail(days).reset_index()


# ------------------------------------------------------------------ 单只行情（持仓估值）
def latest_quote(code: str) -> dict | None:
    """读取某只股票本地缓存的最新一根日线（用于持仓实时估值）"""
    p = os.path.join(DATA_DIR, f"{str(code).zfill(6)}.csv")
    if not os.path.exists(p):
        return None
    try:
        d = pd.read_csv(p)
        if d.empty:
            return None
        r = d.iloc[-1]
        return dict(date=str(r["date"]), close=float(r["close"]),
                    high=float(r["high"]), low=float(r["low"]),
                    open=float(r["open"]))
    except Exception:
        return None


def latest_date_str() -> str:
    """本地缓存中的最新交易日（字符串），从清单缓存快速推断"""
    for f in glob.glob(os.path.join(DATA_DIR, "600000.csv")) or \
             glob.glob(os.path.join(DATA_DIR, "*.csv"))[:1]:
        try:
            d = pd.read_csv(f, usecols=["date"])
            return str(d["date"].max())
        except Exception:
            return ""
    return ""


def fetch_live_quote(code: str) -> dict | None:
    """拉取单只股票最新行情，**不写本地缓存**（供盘中任务使用）。"""
    return fetch_live_quotes([code]).get(str(code).zfill(6))


# ---- 实时快照接口（盘中专用）----
# 说明：新浪日线接口只返回「已收盘」的日K，盘中拿不到当天价格，
#       因此盘中任务改用腾讯/新浪的实时快照接口。
_RT_TENCENT = "http://qt.gtimg.cn/q="
_RT_SINA = "https://hq.sinajs.cn/list="
_RT_HEADERS = {"Referer": "https://finance.sina.com.cn",
               "User-Agent": "Mozilla/5.0"}


def _parse_tencent(txt: str) -> dict:
    out = {}
    for line in txt.strip().split("\n"):
        if '="' not in line:
            continue
        key, body = line.split('="', 1)
        body = body.rstrip('";').strip()
        sym = key.strip().split("_")[-1]          # v_sh600519 → sh600519
        if len(sym) < 8:
            continue
        code6 = sym[2:]
        f = body.split("~")
        if len(f) < 35:
            continue
        try:
            price = float(f[3])
            prev = float(f[4] or 0)
        except Exception:
            continue
        if price <= 0:                            # 停牌/无成交 → 用昨收
            price = prev
        t = f[30]
        out[code6] = dict(
            code=code6, name=f[1],
            date=f"{t[0:4]}-{t[4:6]}-{t[6:8]}" if len(t) >= 8 else "",
            time=f"{t[8:10]}:{t[10:12]}" if len(t) >= 12 else "",
            close=price, prev=prev,
            open=float(f[5] or 0), high=float(f[33] or 0), low=float(f[34] or 0),
            change_pct=float(f[32] or 0), source="腾讯")
    return out


def _parse_sina(txt: str) -> dict:
    out = {}
    for line in txt.strip().split("\n"):
        if '="' not in line:
            continue
        key, body = line.split('="', 1)
        body = body.rstrip('";').strip()
        sym = key.strip().split("_")[-1]
        if len(sym) < 8:
            continue
        code6 = sym[2:]
        f = body.split(",")
        if len(f) < 32 or not f[0]:
            continue
        try:
            price = float(f[3])
            prev = float(f[2] or 0)
        except Exception:
            continue
        if price <= 0:
            price = prev
        out[code6] = dict(
            code=code6, name=f[0], date=f[30], time=(f[31] or "")[:5],
            close=price, prev=prev,
            open=float(f[1] or 0), high=float(f[4] or 0), low=float(f[5] or 0),
            change_pct=round((price - prev) / prev * 100, 2) if prev else 0.0,
            source="新浪")
    return out


def fetch_live_quotes(codes: list[str]) -> dict:
    """批量拉取实时行情 → {code: {...}}。优先腾讯，失败回退新浪。

    一次 HTTP 请求可拿多只，非常适合盘中只盯少数持仓的场景。
    """
    syms, order = [], {}
    for c in codes:
        s = to_sym(c)
        if s:
            syms.append(s)
            order[str(c).zfill(6)] = s
    if not syms:
        return {}

    import requests
    # ① 腾讯（可一次多只）
    try:
        r = requests.get(_RT_TENCENT + ",".join(syms), headers=_RT_HEADERS, timeout=12)
        r.encoding = "gbk"
        got = _parse_tencent(r.text)
        if got:
            return got
    except Exception:
        pass
    # ② 新浪（回退）
    try:
        r = requests.get(_RT_SINA + ",".join(syms), headers=_RT_HEADERS, timeout=12)
        r.encoding = "gbk"
        got = _parse_sina(r.text)
        if got:
            return got
    except Exception:
        pass
    return {}
