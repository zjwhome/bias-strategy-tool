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
import re
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutTimeout
from datetime import datetime

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
# ★ 参数配置文件路径。默认读项目根的 strategy_config.json；
#   可用环境变量 BIA_CONFIG_PATH 指向另一份配置——便于「拿一份改了阈值的配置做测试」
#   而不碰真实配置（与 BIA_DB_PATH 同一套隔离测试思路）。
CONFIG_JSON = os.environ.get("BIA_CONFIG_PATH") or os.path.join(BASE, "strategy_config.json")

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
    main_board_only=True,    # ★ 用户只看沪深主板（见 board_scope）
    breadth_threshold_main=30,   # 主板口径的等效门槛（历史推导：主板≥30 ≈ 全市场≥50，见 strategy_config.board_scope）
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
        bs = j.get("board_scope", {})
        if bs.get("main_board_only") is not None:
            cfg["main_board_only"] = bool(bs["main_board_only"])
        tmm = (rg.get("presets", {}).get(key, {}) or {}).get("threshold_main_board")
        if tmm:
            cfg["breadth_threshold_main"] = int(tmm)
    except Exception as e:
        print(f"[config] 读取失败，使用默认值：{e}")
    return cfg


CFG = load_config()

# ★ 配置热更新：CFG 是「导入时读一次」的模块级字典，一旦服务起来就被冻结。
#   但用户改了 strategy_config.json 里的门槛/乖离率之后，网页上显示的却还是旧值，
#   必须重启服务才生效 —— 这跟「改完刷新页面即生效」的约定不符（出场参数早就做到了）。
#   这里提供 reload_config()，**原地更新**同一个字典对象，
#   于是所有 `core.CFG[...]` / `CFG[...]` 的取值处都会立刻看到新值。
_CFG_LOCK = threading.Lock()
_CFG_AT = 0.0


def reload_config(min_interval: float = 3.0) -> dict:
    """重新读配置文件并原地刷新 CFG，返回 CFG。

    min_interval：最小重读间隔（秒）。/api/status 每 8 秒被轮询一次，
    有这道节流就不会每次都去碰磁盘。任务开始前也会调用它。
    """
    global _CFG_AT
    now = time.time()
    with _CFG_LOCK:
        if now - _CFG_AT < min_interval:
            return CFG
        _CFG_AT = now
        try:
            CFG.update(load_config())          # ★ 原地更新，不换对象
        except Exception as e:
            print(f"[config] 热更新失败，继续用旧值：{e}")
    return CFG


# ------------------------------------------------------------------ 板块归属
# ★ 用户明确要求：**只做沪深主板**，创业板 / 科创板 / 北交所一律不考虑。
#   注意这里的定位：主板限定作用于「**买入名单**」（候选股、盘中参考清单）；
#   而「信号广度」是市场级恐慌温度计，仍按全市场口径统计，
#   这样才和六年回测（门槛 50 → 胜率 85.6%）保持同一把尺子。
#   ★ 两套口径的区别见 memory/NOTES-ops.md 与 webapp/README 的说明。
MB_PREFIXES = ("600", "601", "603", "605",      # 沪市主板
               "000", "001", "002", "003")      # 深市主板（002/003 为原中小板，已并入主板）


def board_of(code) -> str:
    """股票代码 → 所属板块名称

    ⚠️ 302 这个号段容易被误判成主板，实测确认它属于**创业板**：
       「中航成飞」原代码 300114（中航电测），2025-02-17 起改码为 302132，
       公司公告明确依据《深圳证券交易所上市公司自律监管指引第 2 号
       ——创业板上市公司规范运作》，即**上市板仍是创业板**，只是换了号。
       所以 302 必须归到创业板，绝不能算进用户只做的主板。
    """
    c = str(code).zfill(6)
    if c.startswith(("600", "601", "603", "605")):
        return "沪市主板"
    if c.startswith(("000", "001", "002", "003")):
        return "深市主板"
    if c.startswith(("300", "301", "302")):
        return "创业板"
    if c.startswith(("688", "689")):
        return "科创板"
    if c.startswith(("4", "8", "9")):
        return "北交所"
    return "其它"


def is_main_board(code) -> bool:
    return str(code).zfill(6).startswith(MB_PREFIXES)


def filter_main_board_rows(rows: list[dict]) -> list[dict]:
    """从「候选股 / 清单行」里剔掉非沪深主板。配置关闭 main_board_only 时原样返回。

    ★ 为什么读取层也要过滤一遍（写入层已经过滤了）：
      库里 2026-09-16 之前写入的候选股是「限主板」规则生效之前产生的，
      里面混着创业板股票（实测：300750 宁德时代、300227 光韵达、300274 阳光电源）。
      只在写入层过滤，这些历史行会一直留在页面上，用户会以为工具没听他的要求。
      读取时再拦一道，历史脏数据也就地消失了。
    """
    if not CFG.get("main_board_only", True):
        return rows or []
    return [r for r in (rows or []) if is_main_board(r.get("code"))]


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


# ------------------------------------------------------------------ 交易日历
# ★ 为什么需要：内置调度器（server.py）只按「星期几」判断该不该触发。遇到法定
#   节假日（春节、国庆…）照样会触发一次——盘后任务要白下载 20~25 分钟全市场数据。
#   用交易日历挡掉这类空跑。
#   设计原则：**拿不到就放行**。宁可多跑一次，也绝不能因为日历拉不到而漏跑。
_TRADE_CAL = os.path.join(DATA_DIR, "_trade_calendar.json")
_CAL_MEM: dict = {"dates": None}


def trade_calendar(force: bool = False) -> set:
    """沪深交易日集合（"YYYY-MM-DD"）。内存 + 磁盘两级缓存，失败返回空集合。"""
    today = datetime.now().strftime("%Y-%m-%d")
    mem = _CAL_MEM.get("dates")
    if not force and mem and max(mem) >= today:
        return mem

    if not force:
        try:
            with open(_TRADE_CAL, encoding="utf-8") as f:
                d = {str(x)[:10] for x in (json.load(f).get("dates") or [])}
            if d and max(d) >= today:          # 已有日历覆盖到今天 → 直接用
                _CAL_MEM["dates"] = d
                return d
        except Exception:
            pass

    try:
        import akshare as ak
        df = ak.tool_trade_date_hist_sina()
        col = "trade_date" if "trade_date" in df.columns else df.columns[0]
        d = {str(x)[:10] for x in df[col].tolist()}
        if d:
            _CAL_MEM["dates"] = d
            try:
                with open(_TRADE_CAL, "w", encoding="utf-8") as f:
                    json.dump(dict(updated_at=datetime.now().isoformat(timespec="seconds"),
                                   dates=sorted(d)), f)
            except Exception:
                pass
            return d
    except Exception as e:
        print(f"[calendar] 交易日历获取失败：{type(e).__name__}: {e}")
    return mem or set()


def is_trade_day(date_str: str = "") -> bool | None:
    """True=交易日 / False=非交易日 / **None=无法判断（调用方应放行）**。"""
    ds = (date_str or datetime.now().strftime("%Y-%m-%d"))[:10]
    cal = trade_calendar()
    if not cal or max(cal) < ds:
        return None
    return ds in cal


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
    """计算指定交易日的信号广度。

    ★ 同时给出两个口径：
      · breadth        = 全市场（**择时用的就是它**，与六年回测同一把尺子）
      · breadth_main   = 其中属于沪深主板的部分（＝用户真正可以买的池子）
    """
    date = date or latest_trade_date(data)
    d = data[data["date"] == date]
    sig = d[signal_mask(d)]
    all_sig = d[d["bias"] <= CFG["bias_threshold"]]           # 仅乖离率条件，做参考
    mb = sig["code"].map(is_main_board)
    mb_all = all_sig["code"].map(is_main_board)
    th = CFG["breadth_threshold"]
    return dict(
        date=str(pd.Timestamp(date).date()),
        breadth=int(len(sig)),
        bias_only=int(len(all_sig)),
        breadth_main=int(mb.sum()),
        bias_only_main=int(mb_all.sum()),
        threshold=th,
        triggered=bool(len(sig) >= th),
        stocks_total=int(len(d)),
    )


def signal_rows(data: pd.DataFrame, date: pd.Timestamp | None = None,
                board_only: bool | None = None) -> pd.DataFrame:
    """当日满足全部条件的股票（未排序、未截断）。board_only 默认取配置。"""
    date = date or latest_trade_date(data)
    d = data[data["date"] == date]
    sig = d[signal_mask(d)].copy()
    if board_only is None:
        board_only = bool(CFG.get("main_board_only", True))
    if board_only and not sig.empty:
        sig = sig[sig["code"].map(is_main_board)]
    return sig


def pick_candidates(data: pd.DataFrame, date: pd.Timestamp | None = None, limit: int = 10,
                    board_only: bool | None = None) -> pd.DataFrame:
    """按 BIAS 升序取候选股（跌得最狠的在前），并补上股票名称。

    ★ 默认只保留沪深主板——用户明确「其他板不考虑」。想拿全市场名单就传 board_only=False。
    """
    sig = signal_rows(data, date, board_only)
    if sig.empty:
        return sig
    sig = sig.sort_values("bias").head(limit)
    sig["name"] = sig["code"].map(load_names())
    cols = ["code", "name", "close", "bias", "turnover_pct", "vol_ratio", "chg5d", "amount"]
    sig = sig[[c for c in cols if c in sig.columns]]
    return sig.reset_index(drop=True)


def breadth_history(data: pd.DataFrame, days: int = 250) -> pd.DataFrame:
    """历史广度序列（用于画曲线）：每个交易日的广度 + 仅乖离率命中数 + 主板口径"""
    m = signal_mask(data)
    b_only = data["bias"] <= CFG["bias_threshold"]
    mb = data["code"].map(is_main_board)
    hist = data.groupby("date").size().rename("total").to_frame()
    br = data[m].groupby("date").size().rename("breadth")
    bo = data[b_only].groupby("date").size().rename("bias_only")
    brm = data[m & mb].groupby("date").size().rename("breadth_main")
    bom = data[b_only & mb].groupby("date").size().rename("bias_only_main")
    out = hist.join(br, how="left").join(bo, how="left")
    out = out.join(brm, how="left").join(bom, how="left").fillna(0)
    for c in ("breadth", "bias_only", "breadth_main", "bias_only_main"):
        out[c] = out[c].astype(int)
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
        # ★ 盘中扫描需要的额外字段（腾讯快照下标已实测确认）：
        #   [36] 成交量(手)   [57] 成交额(万元，精确)   [38] 换手率(%)
        #   [45] 总市值(亿元)  [49] 量比（腾讯自己的口径，仅作交叉校验）
        def _f(i, d=0.0):
            try:
                return float(f[i])
            except Exception:
                return d
        out[code6] = dict(
            code=code6, name=f[1],
            date=f"{t[0:4]}-{t[4:6]}-{t[6:8]}" if len(t) >= 8 else "",
            time=f"{t[8:10]}:{t[10:12]}" if len(t) >= 12 else "",
            close=price, prev=prev,
            open=float(f[5] or 0), high=float(f[33] or 0), low=float(f[34] or 0),
            change_pct=float(f[32] or 0), source="腾讯",
            vol_hand=_f(36),                       # 当日累计成交量（手）
            amount_wan=_f(57) or _f(37),           # 当日累计成交额（万元）
            turnover_pct=_f(38),                   # 换手率(%)
            mv_total_yi=_f(45),                    # 总市值（亿元）
            vol_ratio_rt=_f(49),                   # 量比（腾讯口径）
        )
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


# ================================================================== 盘中全市场扫描
"""
为什么需要它：策略的「广度」是收盘口径，盘中拿不到。但用户盘中（如 14:30）就
需要有决策依据，所以这里用**实时快照**把当天的广度「推演」出来。

三个口径必须说清楚，否则会严重误判：

① 乖离率 —— 用「前 23 根已收盘价之和 + 今日实时价」/24 推 MA24。
   （前 23 根= 今日之前最近的 23 个交易日，正好凑满 24 日均线）
② 量能   —— 实时成交量是「半天的量」，必须按**已开盘时间进度**折算成全日量，
              否则盘中量比必然只有收盘的一半 → 达标股会少得离谱。
              折算后与策略里的 vol_ratio = 全日量 / 5日均量 完全同口径。
③ 成交额 —— 同上折算。

时间进度以**快照自带的时间戳**为准（不是本机时钟），收盘后自动变成 100%。
"""
TRADE_TOTAL_MIN = 240          # 9:30-11:30 + 13:00-15:00 = 240 分钟
_MIN_PROGRESS = 0.05           # 开盘不到 12 分钟就不做折算（噪声太大）


def market_minutes(h: int, m: int) -> int:
    """当日已开盘分钟数（0~240）。用于把盘中量/额折算成全日口径。"""
    t = int(h) * 60 + int(m)
    if t <= 570:            # 9:30 前
        return 0
    if t <= 690:            # 上午 9:30-11:30
        return t - 570
    if t < 780:             # 午休
        return 120
    if t <= 900:            # 下午 13:00-15:00
        return 120 + (t - 780)
    return 240              # 收盘后


def time_progress(h: int | None = None, m: int | None = None) -> float:
    """开盘时间进度 0~1。不传参就用本机当前时间。"""
    if h is None:
        now = datetime.now()
        h, m = now.hour, now.minute
    return round(min(1.0, max(0.0, market_minutes(h, m) / TRADE_TOTAL_MIN)), 4)


# ---- 盘中基准（每只股票「今日之前」的那部分日线）----
# 字段：前 23 根收盘价之和（配今日实时价推 MA24）、前 4/5 日成交量之和、5 日前收盘价、上市天数
_BASE_CSV = os.path.join(DATA_DIR, "_intraday_base.csv")
_BASE_META = os.path.join(DATA_DIR, "_intraday_base.json")
_BASE_COLS = ["code", "name", "data_date", "n_prior", "sum23", "sum_v4", "sum_v5",
              "close5", "last_close"]
# ★ 缓存结构版本号。**改了 _BASE_COLS 就必须 +1**：否则旧缓存里没有新列，
#   读进来全是 NaN，量比会静默变成 0（不报错、不崩溃，只是结果全错）。
_BASE_SCHEMA = "v2"
# 哨兵日期：当作「今天在很远的未来」→ 尾部每一根 K 线都算「今日之前」。
# 用途见 intraday_base：让基准只随「数据内容」变化，从而跨天复用。
_ALL_BARS = "9999-12-31"


def save_intraday_base(recs: list[dict], cache_key: str = "") -> None:
    """把盘中基准落盘。

    ★ cache_key 见 intraday_base：f"{本地数据最新交易日}|{生效口径}"。
      · 本地数据前进了一天（跑了盘后任务）→ 基准必须跟着前移
      · 口径从 "all" 变成具体扫描日（当天盘后已跑）→ 也必须重建

    ⚠️ 重建耗时**严重依赖系统文件缓存**：热缓存 2.9 秒，冷启动（每天第一次）
       **160.2 秒** —— 5006 个文件逐个 open/seek，实测 2026-09-17。
       「一天才一次、3 秒而已」是错的，这曾经让用户每天白等 2 分半。
       所以现在由盘后任务在数据落地时顺手重建（那时文件刚写过），
       盘中扫描只读缓存。
    """
    try:
        df = pd.DataFrame(recs, columns=_BASE_COLS)
        df.to_csv(_BASE_CSV, index=False, encoding="utf-8")
        with open(_BASE_META, "w", encoding="utf-8") as f:
            json.dump(dict(cache_key=cache_key, n=len(df),
                           built_at=datetime.now().isoformat(timespec="seconds")), f)
    except Exception as e:
        print(f"[intraday_base] 落盘失败：{e}")


def _metrics_from_tail(path: str, today: str, n_bytes: int = 8192) -> dict | None:
    """只读 CSV **尾部**，拿到该股「今日之前」的基准指标。

    ★ 为什么不用 load_dataset：全量汇总要读 1578 万行、约 4 分钟，
      而盘中每日都要算一次，必须压到秒级。每只股票只需要最后 24 行，
      所以按字节 seek 到文件尾部读取即可（快两个数量级）。
    """
    try:
        size = os.path.getsize(path)
        with open(path, "rb") as f:
            start = max(0, size - n_bytes)
            f.seek(start)
            raw = f.read()
    except Exception:
        return None
    lines = [l for l in raw.decode("utf-8-sig", "replace").splitlines() if l.strip()]
    if start > 0 and lines:
        lines = lines[1:]                    # 首行很可能是被截断的半行 → 丢掉
    if not lines:
        return None
    import csv as _csv
    import io as _io
    rows = list(_csv.reader(_io.StringIO("\n".join(lines))))
    if not rows:
        return None
    idx = None
    if not re.match(r"^\d{4}-\d{2}-\d{2}", (rows[0][0] or "").strip()):
        idx = {c.strip().lstrip("\ufeff"): i for i, c in enumerate(rows[0])}
        rows = rows[1:]
    rows = [r for r in rows if r and len(r) >= 6]
    if not rows:
        return None
    i_d = (idx or {}).get("date", 0)
    i_c = (idx or {}).get("close", 4)
    i_v = (idx or {}).get("volume", 5)
    recs = []
    for r in rows[-40:]:
        try:
            dt = str(r[i_d])[:10]
            if not re.match(r"^\d{4}-\d{2}-\d{2}", dt):
                continue
            recs.append((dt, float(r[i_c]), float(r[i_v])))
        except Exception:
            continue
    prior = [x for x in recs if x[0] < today]
    # ★ 收盘口径 ma = rolling(24, min_periods=24)，今日有值只需「今日之前 ≥ 23 根」。
    #   写成 ≥24 会多卡掉一整档股票，与盘后结果对不齐。
    if len(prior) < CFG["bias_period"] - 1:
        return None
    # 上市交易日数：尾部读不到总行数，得整文件数一遍。
    #   ⚠️ 旧写法先用「89 字节/行」估算、只在估算 <260 时才精算 —— 但老股票早年
    #      价格位数少、行长只有 ~81 字节，实测系统性低估约 10%
    #      （000006 估 7394、实际 8160；全市场 2486 只老股票中位低 1.7%、最大 11.5%）。
    #   ★ 现在**无条件精算**：实测字节级 count(b"\n") 扫完 5017 个 CSV 约 4 秒
    #     （旧的逐行正则要 15.5 秒），相对基准构建本身完全可以忽略，
    #     换来的是上市天数永远精确 ——「上市 ≥60 日」这条门槛不再依赖任何估算。
    est_rows = 0
    try:
        with open(path, "rb") as f2:
            n_lines = f2.read().count(b"\n")
        # 第 1 行是表头，其余每行一根 K 线（pandas 落盘保证末尾有换行）
        est_rows = max(0, int(n_lines) - 1 - (len(recs) - len(prior)))
    except Exception:
        pass
    n_prior = max(len(prior), est_rows)
    cl = [x[1] for x in prior]
    vo = [x[2] for x in prior]
    # ★★ sum_v4 / sum_v5 是"量"而不是"均量"：量比的分母窗口要和收盘口径对齐，
    #    而收盘用的是 rolling(5)（**含当日**），当日那根在盘中只能用"预测量"代替，
    #    所以必须留成"和"让推演阶段自己拼分母（详见 intraday_scan 的注释）。
    return dict(data_date=prior[-1][0], n_prior=int(n_prior),
                sum23=round(float(sum(cl[-23:])), 4),
                sum_v4=round(float(sum(vo[-4:])), 2),
                sum_v5=round(float(sum(vo[-5:])), 2),
                close5=round(float(cl[-5]), 4),
                last_close=round(float(cl[-1]), 4))


def intraday_base(data_date: str = "", force: bool = False, log=None,
                  scan_day: str = "", include_all: bool | None = None) -> dict:
    """{code: 盘中基准}。优先读缓存，过期或缺失才重建。

    ★★ 缓存键 = f"{数据日期}|{生效口径}"，口径只有两种：
      · "all" —— 扫描日 > 数据日期。**正常盘中就是这一种**：当天还没收盘，
                 数据源也还没发布当日 K 线，于是「今日之前」= CSV 尾部全部 K 线，
                 基准**只取决于 CSV 内容**，因此可以跨天复用。
      · 扫描日 —— 扫描日 ≤ 数据日期（当天盘后任务已经跑过、当日 K 线已入库）。
                 此时必须把当日那根排除掉，不能复用 "all" 的结果。

    ★★ 为什么非这么掰不可（2026-09-17 实测）：
       旧口径把扫描日**无条件**写进缓存键 → 每天第一次盘中扫描必然全量重建，
       冷启动逐只打开 5006 个 CSV 要 **160.2 秒**（日志原话：
       「基准建立完成：5006 只，耗时 160.2 秒」），而热缓存只要 2.9 秒。
       这 160 秒正是用户 14:30 点完「立即执行」后干等的时间。
       改成 "all" 之后基准随「数据落地」而变（由盘后任务顺手重建，见 updater），
       盘中扫描直接命中缓存。
    """
    log = log or (lambda s: None)
    day = scan_day or datetime.now().strftime("%Y-%m-%d")
    if include_all is None:            # None = 按扫描日自动判断；True/False = 强制
        include_all = day > (data_date or "")
    key = f"{_BASE_SCHEMA}|{data_date or ''}|{'all' if include_all else day}"
    if not force and os.path.exists(_BASE_CSV) and os.path.exists(_BASE_META):
        try:
            with open(_BASE_META, encoding="utf-8") as f:
                meta = json.load(f)
            if str(meta.get("cache_key") or "") == key:
                df = pd.read_csv(_BASE_CSV, dtype={"code": str})
                # ★ 双保险：万一缓存文件被旧版本写过（列不全），宁可重建也不要带着 NaN 往下走
                missing = [c for c in _BASE_COLS if c not in df.columns]
                if missing:
                    log(f"[盘中] 基准缓存缺列 {missing} → 重建")
                else:
                    df["code"] = df["code"].str.zfill(6)
                    return {r["code"]: r for r in df.to_dict("records")}
            else:
                log(f"[盘中] 基准缓存键 {meta.get('cache_key')} ≠ {key} → 重建")
        except Exception as e:
            log(f"[盘中] 基准缓存不可用（{e}），改为重建")

    log("[盘中] 正在建立盘中基准（逐只读取最近 24 根日线）…")
    t0 = time.time()
    out = {}
    files = [f for f in glob.glob(os.path.join(DATA_DIR, "*.csv"))
             if not os.path.basename(f).startswith("_")]
    # ★ 用哨兵当作「今天」→ 尾部所有 K 线都算「今日之前」。
    #   已验证与「扫描日 > 数据日期」时的现口径逐字段完全等价（600 只抽样零差异）。
    eff_today = _ALL_BARS if include_all else day
    names = load_names()
    for f in files:
        code = os.path.basename(f)[:-4]
        if not code.isdigit():
            continue
        m = _metrics_from_tail(f, eff_today)
        if m:
            out[code] = dict(code=code, name=names.get(code, ""), **m)
    if out:
        save_intraday_base(list(out.values()), key)
    log(f"[盘中] 基准建立完成：{len(out)} 只，耗时 {time.time()-t0:.1f} 秒")
    return out


def prebuild_intraday_base(data_date: str, log=None) -> int:
    """盘后数据落地后，顺手把「all」口径的盘中基准重建好。返回只数（失败 -1）。

    ★★ 为什么必须挪到这里来做（2026-09-17 实测）：盘中基准要逐只打开 5006 个
       CSV 读尾部，**冷启动 160.2 秒**、热缓存 2.9 秒。放在盘中任务里重建，
       就是用户 14:30 点完「立即执行」后干等两分半；放在这里，这些文件刚被
       更新器写过（还在系统文件缓存里，快得多），而且这段时间用户本来就在等
       盘后任务（20~25 分钟），多几秒无感。预建好后次日盘中扫描直接命中缓存。
    """
    log = log or (lambda s: None)
    try:
        b = intraday_base(data_date=data_date, force=True, log=log, include_all=True)
        log(f"[盘后] 盘中基准已预建 {len(b)} 只 —— 下次盘中扫描可直接命中缓存")
        return len(b)
    except Exception as e:
        # ★ 预建失败绝不能影响盘后任务本体：次日盘中扫描发现缓存键不匹配会自己重建，
        #   只是慢一点（160 秒），不会算错。
        log(f"[盘后] 盘中基准预建失败（不影响本次更新）：{type(e).__name__}: {e}")
        return -1


def fetch_live_quotes_bulk(codes: list[str], chunk: int = 300, workers: int = 6,
                           log=None) -> dict:
    """分批并发拉取全市场实时快照 → {code: {...}}。

    腾讯接口一次可以带多只，但 URL 不能无限长（5017 只拼一起约 45KB 会被拒），
    因此按 300 只一批、6 批并发。全市场约 17 批，通常 10~30 秒完成。
    """
    log = log or (lambda s: None)
    syms = [s for s in (to_sym(c) for c in codes) if s]
    if not syms:
        return {}
    chunks = [syms[i:i + chunk] for i in range(0, len(syms), chunk)]
    out: dict = {}
    import requests

    def one(cs):
        r = requests.get(_RT_TENCENT + ",".join(cs), headers=_RT_HEADERS, timeout=15)
        r.encoding = "gbk"
        return _parse_tencent(r.text)

    ex = ThreadPoolExecutor(max_workers=workers)
    futs = [ex.submit(one, cs) for cs in chunks]
    try:
        for fu in as_completed(futs, timeout=180):
            try:
                out.update(fu.result() or {})
            except Exception:
                pass
    except FutTimeout:
        log(f"[盘中] 快照有批次超时，已到手 {len(out)} 只，用现有数据继续")
    finally:
        ex.shutdown(wait=False, cancel_futures=True)
    return out


def _shares_today(q: dict) -> float:
    """当日成交量（**股**）。

    ★★ 踩过的坑：腾讯快照里「成交量」的单位**不统一** ——
       主板/创业板是「手」(×100 股)，而**科创板(688/689)是「股」**（实测：
       688356 返回 2,286,700，与本地日线的 2,286,700 完全一致）。
       若统一按「手」处理，科创板量比会被放大 **100 倍**，
       直接导致一批根本没放量的科创板股票被误判成「达标」。
       ⚠️ 2026-09-16 实测就出现了这个假阳性（5 只达标里有 4 只是 688）。

    这里**不去猜代码前缀**，而是用「成交额 ÷ 成交量」反推：当日均价(≈VWAP)
    必须落在当日最低价~最高价之间。这个判据对涨跌停股同样成立。
    """
    v = float(q.get("vol_hand") or 0)
    if v <= 0:
        return 0.0
    amt = float(q.get("amount_wan") or 0) * 1e4
    lo, hi = float(q.get("low") or 0), float(q.get("high") or 0)
    if amt > 0 and hi > 0 and lo > 0:
        as_share = amt / v              # 单位是「股」时的隐含均价
        as_hand = amt / (v * 100.0)     # 单位是「手」时的隐含均价
        ok_share = lo * 0.99 <= as_share <= hi * 1.01
        ok_hand = lo * 0.99 <= as_hand <= hi * 1.01
        if ok_share and not ok_hand:
            return v
        if ok_hand and not ok_share:
            return v * 100.0
    # 兜底：按主流口径（手）处理
    return v * 100.0


def _miss_reasons(price, vol_ratio, chg5d, amount, bar_no) -> list[str]:
    """「仅乖离率符合」的股票还差哪几项 —— 让用户知道是差在量上还是差在别处。"""
    miss = []
    if vol_ratio is None or vol_ratio <= 0:
        miss.append("无成交（停牌？）")
    elif vol_ratio < CFG["vol_surge"]:
        miss.append(f"量能未放大（{vol_ratio:.2f} < {CFG['vol_surge']}）")
    if chg5d is None:
        miss.append("近5日涨跌未知")
    elif chg5d >= 0:
        miss.append(f"近5日没跌（{chg5d:+.2f}%）")
    if amount is None:
        miss.append("成交额未知")
    elif amount < CFG["min_amount"]:
        miss.append(f"成交额不足（{amount/1e8:.2f} < {CFG['min_amount']/1e8:.2f} 亿）")
    if bar_no is not None and bar_no < CFG["min_list_days"]:
        miss.append(f"上市不足 {CFG['min_list_days']} 日（{bar_no} 日）")
    return miss


def intraday_scan(data_date: str = "", log=None, top: int = 30,
                  codes: list[str] | None = None) -> dict:
    """盘中全市场扫描：用实时快照推演「今天收盘时的」广度与两类参考清单。

    返回 candidates = 全套条件达标（＝盘中口径的候选股）
         bias_only  = 只有乖离率达标、别的不满足（附「还差哪一项」）
    """
    log = log or (lambda s: None)
    today = datetime.now().strftime("%Y-%m-%d")
    base = intraday_base(data_date=data_date, log=log, scan_day=today)
    if not base:
        return dict(ok=False, msg="本地还没有可用的日线数据，请先跑一次「盘后任务」。")

    uni = load_universe()
    all_codes = codes or uni["code"].tolist()
    t0 = time.time()
    log(f"[盘中] 拉取全市场实时快照（{len(all_codes)} 只）…")
    rt = fetch_live_quotes_bulk(all_codes, log=log)
    log(f"[盘中] 收到 {len(rt)} 只快照，耗时 {time.time()-t0:.1f} 秒")
    if len(rt) < len(all_codes) * 0.5:
        return dict(ok=False, msg=f"实时快照只取到 {len(rt)}/{len(all_codes)} 只，"
                                  "网络异常，请稍后重试。")

    # ---- 时间进度：以快照自带时间戳为准（收盘后自然就是 100%）----
    tcnt: dict = {}
    for q in rt.values():
        if q.get("date") == today and q.get("time"):
            tcnt[q["time"]] = tcnt.get(q["time"], 0) + 1
    early = False
    if tcnt:
        hhmm = max(tcnt, key=tcnt.get)
        try:
            h, mi = (int(x) for x in hhmm.split(":")[:2])
        except Exception:
            h, mi = datetime.now().hour, datetime.now().minute
        prog = time_progress(h, mi)
        session, quote_date = "trading", today
        if prog >= 1.0:
            # 快照是今天的，但时间已过 15:00 —— 此时"实时价"就是收盘价，
            # 口径已等同于收盘，前端要说「非交易时段快照」而不是「盘中实时」。
            session = "closed"
        elif prog < _MIN_PROGRESS:
            prog, early = _MIN_PROGRESS, True
        q_time = hhmm
    else:
        # 快照不是今天 → 非交易日 / 盘后取到的是上一交易日收盘价
        prog = 1.0
        session = "closed"
        q_time = ""
        quote_date = max((q.get("date") or "" for q in rt.values()), default="")

    th = CFG["breadth_threshold"]
    mb_only = bool(CFG.get("main_board_only", True))
    hits, near = [], []            # ★ 主板口径（用户只看这些）
    hits_full = near_full = 0      # 全市场口径（与门槛 50 / 六年回测对照）
    n_scanned = 0
    base_date = ""
    cmp_n = cmp_bad = 0            # 自算量比 vs 腾讯量比 的一致性自检
    for code, q in rt.items():
        b = base.get(code)
        if not b:
            continue
        price = float(q.get("close") or 0)
        if price <= 0:
            continue
        # ★ 最少 K 线数：收盘口径 ma = rolling(24, min_periods=24)，今日有值需要
        #   「今日之前 ≥ 23 根」。写成 ≥24 会多卡掉 1 根，与盘后结果对不齐。
        n_prior = int(b.get("n_prior") or 0)
        if n_prior < CFG["bias_period"] - 1:
            continue
        ma24 = (float(b["sum23"]) + price) / 24.0
        if ma24 <= 0:
            continue
        n_scanned += 1
        bd = str(b.get("data_date") or "")
        if bd > base_date:
            base_date = bd

        sum_v4 = float(b.get("sum_v4") or 0)
        sum_v5 = float(b.get("sum_v5") or 0)
        vol_today = _shares_today(q)                            # ★ 已按板块修正单位
        est_vol = vol_today / prog                              # 今日全天预估成交量（股）
        # ★★ 量比的分母必须和收盘/回测同尺，否则 14:30 推演和盘后结果对不上：
        #     收盘口径 vol_ma5 = rolling(5).mean() 是**含当日**的（V[T-4..T] 五根），
        #     回测脚本 backtest_bias_full.py 用的是同一写法。
        #     旧实现拿「今日之前 5 根」当分母，窗口整整错开一天 ——
        #     2026-09-17 全市场逐股比对：中位偏差 6.6%、最大 64.5%，
        #     4557/4993 只偏差 >1%，1658 只 >10%。
        #     所以这里把「今日预测量」补进分母：V5 = (前4日量 + 今日预测量) / 5。
        v5 = (sum_v4 + est_vol) / 5.0
        vol_ratio = est_vol / v5 if v5 > 0 else None
        vr_rt = float(q.get("vol_ratio_rt") or 0)
        # ★ 自检：腾讯量比 = (今日至今量 / 已开市分钟) ÷ (过去5日平均每分钟量)
        #     ⇒ 反推「今日全天预测量」= 量比 × 过去5日均量（**不含当日**）。
        #    拿它和我们自己的 est_vol 比，就能校验**单位**是否对了 ——
        #    历史上正是靠这一比对抓到科创板被放大 100 倍的假阳性。
        #    ⚠️ 千万不能拿 vol_ratio 直接比 vr_rt：两者分母窗口定义不同（含/不含当日），
        #       放量越大差得越多（3 倍量时差约 29%），那样会全是误报。
        #       所以比的是"量"，不是"比"。
        #   ⚠️ est_vol == 0 要排除：停牌股当日无成交，拿 0 去比必然 100% 偏离，
        #      那是"没得比"而不是"比错了"，混进来会污染自检的偏离率。
        if vr_rt > 0 and sum_v5 > 0 and est_vol > 0 and prog >= 0.5:
            est_implied = vr_rt * sum_v5 / 5.0
            if est_implied > 0:
                cmp_n += 1
                if abs(est_vol / est_implied - 1) > 0.25:
                    cmp_bad += 1

        bias = (price - ma24) / ma24 * 100
        if bias > CFG["bias_threshold"]:
            continue                                    # 乖离率都没达标，两类清单都不进
        c5 = float(b.get("close5") or 0)
        chg5d = (price / c5 - 1) * 100 if c5 > 0 else None
        amount = float(q.get("amount_wan") or 0) * 1e4 / prog
        # ★ 上市交易日数：收盘口径是 cumcount()，即「今日之前有几根 K 线」，
        #   回测脚本沿用同一定义。所以这里就等于 n_prior，**绝不能 +1** ——
        #   加 1 会让「上市 ≥ 60 日」这条门槛整体宽松一天
        #   （2026-09-17 逐股比对：33 只完整历史的小盘股全部错位 +1）。
        bar_no = n_prior

        # ★ 名称必须规整成字符串：缓存 CSV 里的空 name 读回来是**浮点 NaN**，
        #   而 NaN 在 Python 里是「真值」，`q.get("name") or b.get("name")` 会选中它，
        #   下面那句 `nm.upper()` 立刻抛 AttributeError，整次盘中扫描直接崩掉。
        _qn, _bn = q.get("name"), b.get("name")
        nm = (_qn.strip() if isinstance(_qn, str) else "") \
            or (_bn.strip() if isinstance(_bn, str) else "")
        mb = is_main_board(code)
        rec = dict(code=code, name=nm, board=board_of(code), mb=int(mb),
                   close=round(price, 3),
                   change_pct=round(float(q.get("change_pct") or 0), 2),
                   bias=round(bias, 2),
                   vol_ratio=(round(vol_ratio, 2) if vol_ratio is not None else None),
                   chg5d=(round(chg5d, 2) if chg5d is not None else None),
                   amount=round(amount, 0),
                   turnover_pct=round(float(q.get("turnover_pct") or 0), 2),
                   mv_total_yi=round(float(q.get("mv_total_yi") or 0), 2),
                   vol_ratio_rt=round(float(q.get("vol_ratio_rt") or 0), 2),
                   high=round(float(q.get("high") or 0), 3),
                   low=round(float(q.get("low") or 0), 3),
                   bar_no=bar_no,
                   st=1 if ("ST" in nm.upper() or "退" in nm) else 0)

        ok = ((vol_ratio is not None and vol_ratio >= CFG["vol_surge"])
              and (chg5d is not None and chg5d < 0)
              and amount >= CFG["min_amount"]
              and bar_no >= CFG["min_list_days"])
        if ok:
            hits_full += 1
            if mb:
                hits.append(rec)
        else:
            near_full += 1
            rec["miss"] = _miss_reasons(price, vol_ratio, chg5d, amount, bar_no)
            if mb:
                near.append(rec)

    hits.sort(key=lambda r: r["bias"])
    near.sort(key=lambda r: r["bias"])
    log(f"[盘中] 扫描 {n_scanned} 只：主板达标 {len(hits)} 只 / 仅乖离率符合 {len(near)} 只"
        f"（全市场口径 {hits_full} / {near_full}；时间进度 {prog*100:.1f}%）")
    if cmp_n:
        log(f"[盘中] 量比自检：{cmp_n} 只可比对，偏离>25% 的 {cmp_bad} 只"
            f"（{'正常' if cmp_bad < cmp_n * 0.05 else '⚠ 异常，请检查单位口径'}）")
    else:
        # ★ 说清楚「没比」而不是让下游把 vol_bad=0 读成「比过了、没问题」。
        #   早盘（进度<50%，约 12:30 前）量的外推系数很大，与腾讯口径可比性差，
        #   刻意不比对；但这个"跳过"必须在日志和返回体里显式留痕，
        #   否则一个 0 会被当成绿灯 —— 自检在最需要它的时段反而静默失效。
        log(f"[盘中] 量比自检：本次跳过（时间进度 {prog*100:.1f}% < 50%，"
            f"早盘量能外推噪声大，不参与比对）")

    return dict(
        ok=True, scan_at=datetime.now().isoformat(timespec="seconds"),
        quote_date=quote_date, quote_time=q_time, session=session,
        base_date=base_date, progress=prog, early=early,
        scanned=n_scanned, quotes=len(rt),
        vol_cmp=cmp_n, vol_bad=cmp_bad,
        # ★ 下游判断「自检是否真的跑过」要用这个，不要用 vol_bad==0：
        #   cmp_n=0 时 vol_bad 必然也是 0，那不是通过，是没测。
        vol_checked=bool(cmp_n),
        main_board_only=mb_only,
        breadth=len(hits),                     # ★ 主板口径（用户实际能买的池子）
        bias_only_total=len(near),
        breadth_full=hits_full,                # ★ 全市场口径（与六年回测同一把尺子）
        bias_only_full=near_full,
        threshold=th,
        # ★ 择时总开关用**全市场口径**：门槛 50 是在全市场 5017 只上六年回测出来的，
        #   换成主板口径计数池变小，同一把尺子的刻度就变了（详见 threshold_main）。
        triggered=bool(hits_full >= th),
        threshold_main=CFG.get("breadth_threshold_main"),
        triggered_main=(bool(len(hits) >= CFG["breadth_threshold_main"])
                        if CFG.get("breadth_threshold_main") else None),
        candidates=hits[:top], bias_only=near[:top],
        bias_threshold=CFG["bias_threshold"], vol_surge=CFG["vol_surge"],
        min_amount=CFG["min_amount"], min_list_days=CFG["min_list_days"],
    )
