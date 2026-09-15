"""
================================================================================
乖离率超跌反弹策略 —— 全量回测 v3（修正版）
================================================================================
相对 v2 的修正与升级：
  1. [BUGFIX] 移除 `entry >= n_close*0` 逻辑，原写法恒为 True → 所有交易被过滤
  2. [BUGFIX] 未来乖离率 bias_f{k} 正确计算（用 shift(-k) 的未来均线），
              此前用当日 bias 占位，导致「回归均线止盈」永远不触发
  3. [升级]   候选集只筛一次（bias<-8 且 流动性达标），
              vol_ok / chg5d_ok / idx_ok 改为「统计维度」而非硬过滤 → 免重复模拟
  4. [升级]   出场路径预计算（build_path），8 种出场方案共用，速度大幅提升
  5. [升级]   止损跳空处理：跌破止损且低开 → 按开盘价成交，不再乐观按 -3% 成交
  6. [升级]   O{k} 开盘价序列纳入路径，用于跳空判断

运行：
    全量      python backtest_bias_full.py
    冒烟测试  QUICK=1 python backtest_bias_full.py     (只取 300 只)
================================================================================
"""
import os
import sys
import time
import traceback
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ==================== 【关键修复】串行化 py_mini_racer 构造 ====================
# akshare 的 stock_zh_a_daily（新浪源）**每调用一次就新建一个 MiniRacer(V8 引擎)**。
# 多线程并发创建 V8 上下文时，其内部的 configurable pool 初始化不是线程安全的，
# 会触发原生崩溃并直接杀死进程：
#     FATAL:partition_address_space.cc(243)] Check failed: !IsConfigurablePoolInitialized()
# 修复：给 MiniRacer.__init__ 套一把全局锁，把「创建 V8 上下文」串行化。
# （构造开销很小，实测并发吞吐不受影响）
import threading as _threading

_MR_LOCK = _threading.Lock()


def _patch_mini_racer():
    try:
        from py_mini_racer import MiniRacer as _MR
    except Exception as e:                      # noqa: BLE001
        print(f"[init] py_mini_racer 不可用，跳过补丁：{e}", flush=True)
        return
    if getattr(_MR, "_wb_serialized", False):
        return
    _orig_init = _MR.__init__

    def _serialized_init(self, *args, **kwargs):
        with _MR_LOCK:
            _orig_init(self, *args, **kwargs)

    _MR.__init__ = _serialized_init
    _MR._wb_serialized = True
    print("[init] 已启用 py_mini_racer 构造串行化补丁（防 V8 并发崩溃）", flush=True)


_patch_mini_racer()

QUICK = os.environ.get("QUICK", "0") == "1"

CFG = dict(
    start_date=os.environ.get("BT_START", "2024-08-01"),
    end_date=os.environ.get("BT_END", "2026-08-31"),
    bias_period=24,
    hold_max=10,
    min_amount=float(os.environ.get("BT_MINAMT", "8e7")),   # 信号日 成交额下限（元）
    min_list_days=60,        # 数据不足 60 个交易日 视作次新，剔除
    workers=int(os.environ.get("WORKERS", "8")),
    retry=3,
    data_dir=os.environ.get("BT_DATA", "./data_full"),
    out_dir=os.environ.get("BT_OUT", "./output_full"),
)

BIAS_CAND = -8.0             # 候选集最松阈值，统计时再按 BIAS_GRID 切片
BIAS_GRID = [-10, -12, -13, -14, -15, -16, -17, -18, -20, -22, -25, -28, -30]
VOL_SURGE = 1.5              # 放量确认：成交量 ≥ 5日均量 × 1.5
COST_PCT = 0.4               # 单笔往返成本（佣金+印花税+滑点，%）

# ---------------------------------------------------------------- 出场方案
# targets: [(涨幅%, 触发后累计卖出比例)]；bias_exit: 乖离率回归到该值则清仓
EXIT_VARIANTS = {
    "A_原版·盘中-3%·归零止盈·6日": dict(
        hold=6, stop_mode="intraday", stop_pct=-3.0, targets=[],
        trail_trigger=None, trail_dd=3.0, bias_exit=0.0),
    "B_盘中-3%·分层止盈·10日": dict(
        hold=10, stop_mode="intraday", stop_pct=-3.0,
        targets=[(6.0, 0.5), (10.0, 1.0)], trail_trigger=5.0, trail_dd=3.0, bias_exit=None),
    "C_盘中-3%·分层止盈·6日": dict(
        hold=6, stop_mode="intraday", stop_pct=-3.0,
        targets=[(6.0, 0.5), (10.0, 1.0)], trail_trigger=5.0, trail_dd=3.0, bias_exit=None),
    "D_收盘-4%·分层止盈·10日": dict(
        hold=10, stop_mode="close", stop_pct=-4.0,
        targets=[(6.0, 0.5), (10.0, 1.0)], trail_trigger=5.0, trail_dd=3.0, bias_exit=None),
    "E_盘中-3%·移动止盈为主·6日": dict(
        hold=6, stop_mode="intraday", stop_pct=-3.0, targets=[],
        trail_trigger=4.0, trail_dd=2.5, bias_exit=None),
    "F_盘中-3%·目标减半·10日": dict(
        hold=10, stop_mode="intraday", stop_pct=-3.0,
        targets=[(3.0, 0.5), (9.0, 1.0)], trail_trigger=5.0, trail_dd=3.0, bias_exit=None),
    "G_盘中-3%·分层+归零·10日": dict(
        hold=10, stop_mode="intraday", stop_pct=-3.0,
        targets=[(5.0, 0.5), (9.0, 1.0)], trail_trigger=5.0, trail_dd=3.0, bias_exit=0.0),
    "H_盘中-3%·纯持有·10日(基准)": dict(
        hold=10, stop_mode="intraday", stop_pct=-3.0, targets=[],
        trail_trigger=None, trail_dd=3.0, bias_exit=None),
    "I_收盘-6%·分层止盈·10日": dict(
        hold=10, stop_mode="close", stop_pct=-6.0,
        targets=[(6.0, 0.5), (10.0, 1.0)], trail_trigger=5.0, trail_dd=3.0, bias_exit=None),
    "J_收盘-4%·不止盈·10日(基准)": dict(
        hold=10, stop_mode="close", stop_pct=-4.0, targets=[],
        trail_trigger=None, trail_dd=3.0, bias_exit=None),
}


_ONLY = os.environ.get("BT_VARIANTS", "").strip()
if _ONLY:
    _keys = [k for k in EXIT_VARIANTS if k.split("_")[0] in _ONLY.split(",")]
    if _keys:
        EXIT_VARIANTS = {k: EXIT_VARIANTS[k] for k in _keys}


def log(msg):
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def ensure_dirs():
    os.makedirs(CFG["data_dir"], exist_ok=True)
    os.makedirs(CFG["out_dir"], exist_ok=True)


# ============================== 数据获取 ==============================
def _raw_universe():
    """多源兜底获取 A 股清单（东财快照/北交所官网被代理拦截，优先用新浪）"""
    import akshare as ak
    # 源1：新浪全市场快照（含 sh/sz/bj 前缀）
    for attempt in range(4):
        try:
            s = ak.stock_zh_a_spot()
            raw = s.rename(columns={"代码": "raw", "名称": "name"})[["raw", "name"]]
            raw["code"] = raw["raw"].str.replace(r"^(sh|sz|bj)", "", regex=True).str.zfill(6)
            log(f"  清单来源：新浪快照（{len(raw)} 条）")
            return raw[["code", "name"]]
        except Exception as e:
            log(f"  新浪快照第 {attempt+1} 次失败：{type(e).__name__}")
            time.sleep(2 * (attempt + 1))
    # 源2：交易所官网
    frames = []
    for fn, label in ((ak.stock_info_sh_name_code, "上交所"),
                      (ak.stock_info_sz_name_code, "深交所")):
        try:
            d = fn()
            ccol = next(c for c in d.columns if "代码" in str(c))
            ncol = next(c for c in d.columns if "简称" in str(c) or "名称" in str(c))
            part = d[[ccol, ncol]].rename(columns={ccol: "code", ncol: "name"})
            part["code"] = part["code"].astype(str).str.zfill(6)
            frames.append(part)
            log(f"  清单来源：{label}官网（{len(part)} 条）")
        except Exception as e:
            log(f"  {label}官网失败：{type(e).__name__}")
    if frames:
        return pd.concat(frames, ignore_index=True)
    raise RuntimeError("所有清单来源均失败")


def get_universe():
    log("获取 A 股代码清单 ...")
    cache_u = os.path.join(CFG["data_dir"], "_universe.csv")
    if os.path.exists(cache_u) and os.path.getsize(cache_u) > 500 and not QUICK:
        df = pd.read_csv(cache_u, dtype={"code": str})
        df["code"] = df["code"].str.zfill(6)
        log(f"标的池（本地缓存）：{len(df)} 只")
        return df
    df = _raw_universe()
    df["code"] = df["code"].astype(str).str.zfill(6)
    df["name"] = df["name"].astype(str)
    n0 = len(df)
    df = df[~df["name"].str.contains("ST", na=False)]                 # 剔除 ST（按当前名称，静态口径）
    df = df[~df["name"].str.contains("退", na=False)]                 # 剔除退市整理
    df = df[~df["code"].str.startswith(("4", "8", "92"))]             # 剔除北交所
    df = df[df["code"].str.startswith(("60", "00", "30", "688", "689"))]
    df = df.drop_duplicates("code").reset_index(drop=True)
    if not QUICK:
        df.to_csv(cache_u, index=False, encoding="utf-8-sig")
    log(f"标的池：{n0} → {len(df)} 只（已剔除 ST/退市/北交所）")
    if QUICK:
        df = df.sample(min(300, len(df)), random_state=42).reset_index(drop=True)
        log(f"[QUICK] 抽样子集 {len(df)} 只")
    return df


def fetch_one(code):
    """日线来源：新浪 stock_zh_a_daily（东财 push2his 接口被代理拦截，不可用）
       实测：字段单位 amount=元、turnover=换手率(小数)，并发 x8 单只约 0.2s"""
    import akshare as ak
    cache = os.path.join(CFG["data_dir"], f"{code}.csv")
    if os.path.exists(cache) and os.path.getsize(cache) > 200:
        return True
    sym = ("sh" if code.startswith(("6", "9")) else "sz") + code
    for attempt in range(CFG["retry"]):
        try:
            df = ak.stock_zh_a_daily(symbol=sym,
                                     start_date=CFG["start_date"].replace("-", ""),
                                     end_date=CFG["end_date"].replace("-", ""), adjust="qfq")
            if df is None or df.empty:
                open(cache, "w").write("empty")
                return False
            if "turnover" not in df.columns:
                df["turnover"] = np.nan
            df = df[["date", "open", "close", "high", "low", "volume", "amount", "turnover"]]
            df.to_csv(cache, index=False)
            return True
        except Exception:
            time.sleep(1.0 * (attempt + 1))
    return False


def build_dataset():
    stocks = get_universe()
    done, fail = 0, 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=CFG["workers"]) as ex:
        futs = {ex.submit(fetch_one, c): c for c in stocks["code"]}
        for f in as_completed(futs):
            done += 1
            try:
                if not f.result():
                    fail += 1
            except Exception:
                fail += 1
            if done % 300 == 0 or done == len(stocks):
                el = time.time() - t0
                eta = el / done * (len(stocks) - done) / 60 if done else 0
                log(f"  下载 {done}/{len(stocks)}  失败{fail}  用时{el/60:.1f}min  预计剩余{eta:.1f}min")

    frames = []
    for _, r in stocks.iterrows():
        p = os.path.join(CFG["data_dir"], f"{r['code']}.csv")
        if not os.path.exists(p) or os.path.getsize(p) < 200:
            continue
        try:
            d = pd.read_csv(p)
        except Exception:
            continue
        if d.empty or "close" not in d.columns or len(d) < CFG["min_list_days"]:
            continue
        d["code"] = r["code"]
        d["name"] = r["name"]
        frames.append(d)
    if not frames:
        raise RuntimeError("没有可用数据")
    data = pd.concat(frames, ignore_index=True)
    data["date"] = pd.to_datetime(data["date"])
    for c in ["open", "close", "high", "low", "volume", "amount", "turnover"]:
        if c in data.columns:
            data[c] = pd.to_numeric(data[c], errors="coerce")
    if "turnover" not in data.columns:
        data["turnover"] = np.nan
    data = data.dropna(subset=["open", "close", "high", "low"])
    data = data.sort_values(["code", "date"]).reset_index(drop=True)
    log(f"数据集：{len(data):,} 行 / {data['code'].nunique()} 只 / "
        f"{data['date'].min():%Y-%m-%d} ~ {data['date'].max():%Y-%m-%d}")
    return data


# ============================== 指标计算 ==============================
def compute_features(data):
    log("计算技术指标与远期路径 ...")
    g = data.groupby("code", sort=False)
    n = CFG["bias_period"]
    H = CFG["hold_max"]

    data["ma"] = g["close"].transform(lambda s: s.rolling(n, min_periods=n).mean())
    data["bias"] = (data["close"] - data["ma"]) / data["ma"] * 100
    data["vol_ma5"] = g["volume"].transform(lambda s: s.rolling(5, min_periods=3).mean())
    data["amt_ma5"] = g["amount"].transform(lambda s: s.rolling(5, min_periods=3).mean())
    data["chg5d"] = g["close"].transform(lambda s: s.pct_change(5) * 100)

    data["n_open"] = g["open"].shift(-1)
    data["n_close"] = g["close"].shift(-1)

    gd = data.groupby("code", sort=False)
    for k in range(1, H + 1):
        data[f"L{k}"] = gd["low"].shift(-k).astype("float32")
        data[f"H{k}"] = gd["high"].shift(-k).astype("float32")
        data[f"C{k}"] = gd["close"].shift(-k).astype("float32")
        data[f"O{k}"] = gd["open"].shift(-k).astype("float32")
        ma_f = gd["ma"].shift(-k)
        data[f"bias_f{k}"] = ((data[f"C{k}"] - ma_f) / ma_f * 100).astype("float32")

    # 放量确认（信号日）
    data["vol_ok"] = data["volume"] >= data["vol_ma5"] * VOL_SURGE
    data["chg5d_ok"] = data["chg5d"] < 0
    # 换手率（新浪 turnover 为小数，×100 得百分比）
    data["to_pct"] = data["turnover"] * 100

    # ★ 停牌防护：持有窗口内若「相邻两个交易日」间隔 >12 个自然日，说明中途停牌/复牌，
    #    这类交易不可复制（复牌后可能连续一字板），必须剔除，否则严重污染均值。
    gdate = data.groupby("code", sort=False)["date"]
    prev = data["date"]
    gapmax = pd.Series(0, index=data.index, dtype="int32")
    for k in range(1, H + 1):
        nxt = gdate.shift(-k)
        g = (nxt - prev).dt.days
        gapmax = np.maximum(gapmax, g.fillna(0).astype("int32"))
        prev = nxt
    data["gap_ok"] = gapmax <= 12
    return data


def add_index_regime(data):
    """大盘环境：沪深300 收盘 > 其 20 日均线 视为多头环境
       数据源优先级：新浪指数 → 东财指数（东财接口常被代理拦截）"""
    import akshare as ak
    idx = None
    for label, fn in (
        ("新浪", lambda: ak.stock_zh_index_daily(symbol="sh000300")),
        ("东财", lambda: ak.index_zh_a_hist(symbol="000300", period="daily",
                                            start_date=CFG["start_date"].replace("-", ""),
                                            end_date=CFG["end_date"].replace("-", ""))),
    ):
        for attempt in range(2):
            try:
                d = fn()
                if d is not None and not d.empty:
                    idx = d
                    log(f"  指数来源：{label}（{len(d)} 行）")
                    break
            except Exception as e:
                log(f"  指数源 {label} 第{attempt+1}次失败：{type(e).__name__}")
                time.sleep(1.5)
        if idx is not None:
            break

    if idx is None:
        log("[WARN] 大盘环境获取失败（所有源不可用），idx_ok 全部置 True")
        data["idx_ok"] = True
        return data

    ccol = next(c for c in idx.columns if str(c) in ("close", "收盘"))
    dcol = next(c for c in idx.columns if str(c) in ("date", "日期"))
    idx = idx[[dcol, ccol]].rename(columns={dcol: "date", ccol: "iclose"})
    idx["date"] = pd.to_datetime(idx["date"])
    idx = idx[(idx["date"] >= CFG["start_date"]) & (idx["date"] <= CFG["end_date"])]
    idx["ima20"] = idx["iclose"].rolling(20, min_periods=20).mean()
    idx["idx_ok"] = idx["iclose"] > idx["ima20"]
    data = data.merge(idx[["date", "idx_ok"]], on="date", how="left")
    data["idx_ok"] = data["idx_ok"].fillna(True)
    log(f"大盘环境已接入（沪深300>MA20 天数占比 {idx['idx_ok'].mean()*100:.0f}%）")
    return data


# ============================== 交易模拟 ==============================
def build_path(row):
    """把一行信号压缩成后续 H 日的价格路径，供多种出场方案复用"""
    entry = row["n_open"]
    if not np.isfinite(entry) or entry <= 0:
        return None
    code = str(row["code"])
    lim = 1.20 if code.startswith(("300", "301", "688", "689")) else 1.10
    # 次日高开至涨停 → 买不进，跳过
    if entry >= row["close"] * lim * 0.999:
        return None
    H = CFG["hold_max"]
    L = np.array([row[f"L{k}"] for k in range(1, H + 1)], dtype=float)
    Hi = np.array([row[f"H{k}"] for k in range(1, H + 1)], dtype=float)
    C = np.array([row[f"C{k}"] for k in range(1, H + 1)], dtype=float)
    O = np.array([row[f"O{k}"] for k in range(1, H + 1)], dtype=float)
    BF = np.array([row[f"bias_f{k}"] for k in range(1, H + 1)], dtype=float)
    ok = np.isfinite(L) & np.isfinite(Hi) & np.isfinite(C) & np.isfinite(O)
    if not ok.any():
        return None
    return dict(entry=entry, L=L, H=Hi, C=C, O=O, BF=BF, ok=ok,
                date=row["date"], code=row["code"], name=row["name"], bias=row["bias"],
                vol_ok=bool(row["vol_ok"]), chg5d_ok=bool(row["chg5d_ok"]),
                idx_ok=bool(row["idx_ok"]), to_pct=float(row.get("to_pct", np.nan)))


def simulate(p, v):
    """
    出场模拟。关键制度约束：
      · 建仓在 t+1 开盘；A股 T+1 → **t+1 当日不可卖出**，最早 t+2 才能卖
      · 止损：若某日开盘已跌破止损价（跳空）→ 按开盘价成交（因为只能挂单卖）
      · 分层止盈按限价单假设成交在目标价
      · 同日既触碰止损又触碰止盈时，保守假设先触发止损
    """
    entry = p["entry"]
    realized = 0.0      # 累计收益（按卖出权重加权，单位 %）
    sold = 0.0
    peak = 0.0
    hit = [False] * len(v["targets"])
    reason = "到期"
    held = 0

    for k in range(1, v["hold"] + 1):
        if not p["ok"][k - 1]:
            break
        held = k
        lo, hi, cl, op = p["L"][k - 1], p["H"][k - 1], p["C"][k - 1], p["O"][k - 1]

        # 峰值（含建仓当日盘中高点，用于移动止盈的观察基准）
        peak = max(peak, (hi / entry - 1) * 100)

        # ★ T+1：建仓当日（k=1）不可卖出，跳过所有出场判断
        if k < 2:
            continue

        # ---- P0 止损 ----
        if v["stop_mode"] == "intraday":
            sp = v["stop_pct"]
            if op <= entry * (1 + sp / 100.0):
                realized += (1 - sold) * (op / entry - 1) * 100     # 跳空低开 → 开盘价
                sold = 1.0; reason = "止损(跳空)"; break
            if lo <= entry * (1 + sp / 100.0):
                realized += (1 - sold) * sp                          # 盘中触及 → 止损价
                sold = 1.0; reason = "止损"; break
        else:
            sp = v["stop_pct"]
            if cl <= entry * (1 + sp / 100.0):
                realized += (1 - sold) * (cl / entry - 1) * 100
                sold = 1.0; reason = "止损(收盘)"; break

        # ---- P1 分层止盈（限价单）----
        for i, (tg, cum) in enumerate(v["targets"]):
            if not hit[i] and hi >= entry * (1 + tg / 100.0):
                hit[i] = True
                frac = cum - sold
                if frac > 0:
                    realized += frac * tg
                    sold = cum
                    reason = f"止盈{tg:.0f}%"
        if sold >= 1.0:
            break

        cur = (cl / entry - 1) * 100

        # ---- P2 移动止盈 ----
        if v["trail_trigger"] is not None and peak >= v["trail_trigger"]:
            if cur <= peak - v["trail_dd"]:
                realized += (1 - sold) * cur
                sold = 1.0; reason = "移动止盈"; break

        # ---- P3 乖离率回归止盈 ----
        if v["bias_exit"] is not None:
            fb = p["BF"][k - 1]
            if np.isfinite(fb) and fb >= v["bias_exit"]:
                realized += (1 - sold) * cur
                sold = 1.0; reason = "回归均线"; break

    if sold < 1.0:
        idx = max(held, 1) - 1
        cl = p["C"][idx] if np.isfinite(p["C"][idx]) else entry
        realized += (1 - sold) * (cl / entry - 1) * 100
        if reason == "到期":
            reason = f"到期({held}日)"

    return dict(date=p["date"], code=p["code"], name=p["name"], bias=round(p["bias"], 2),
                entry=round(entry, 2), ret=round(realized, 2), ret_net=round(realized - COST_PCT, 2),
                hold=held, reason=reason, vol_ok=p["vol_ok"], chg5d_ok=p["chg5d_ok"],
                idx_ok=p["idx_ok"], to_pct=p["to_pct"])


def run_variant(paths, name, v):
    trades = []
    for p in paths:
        t = simulate(p, v)
        if t:
            trades.append(t)
    df = pd.DataFrame(trades)
    return df


# ============================== 统计 ==============================
def stats(df):
    """统计口径说明：
       · 均收益     —— 算术平均，易被极端值（连续涨停/复牌）拉高
       · 均收益截尾 —— 单笔收益截断在 ±20% 再取平均（稳健口径，作为主要判据）
       · 中位       —— 中位数，反映"典型"一笔交易的体验
    """
    empty = dict(信号数=0, 胜率=np.nan, 均收益=np.nan, 均收益截尾=np.nan, 中位=np.nan,
                 盈亏比=np.nan, 均盈=np.nan, 均亏=np.nan, 最好=np.nan, 最差=np.nan,
                 止损率=np.nan, 平均持有=np.nan)
    if df is None or df.empty:
        return empty
    r = df["ret_net"]
    win, los = r[r > 0], r[r <= 0]
    gp, gl = win.sum(), abs(los.sum())
    return dict(
        信号数=len(df),
        胜率=round((r > 0).mean() * 100, 1),
        均收益=round(r.mean(), 2),
        均收益截尾=round(r.clip(-20, 20).mean(), 2),
        中位=round(r.median(), 2),
        盈亏比=round(gp / gl, 2) if gl > 0 else 99.0,
        均盈=round(win.mean(), 2) if len(win) else 0.0,
        均亏=round(los.mean(), 2) if len(los) else 0.0,
        最好=round(r.max(), 1),
        最差=round(r.min(), 1),
        止损率=round(df["reason"].str.startswith("止损").mean() * 100, 1),
        平均持有=round(df["hold"].mean(), 1),
    )


def main():
    ensure_dirs()
    data = build_dataset()
    data = compute_features(data)
    data = add_index_regime(data)

    # 候选集：最松阈值 + 流动性门槛 + 停牌防护（其余条件作为统计维度）
    pre = data[(data["bias"] < BIAS_CAND) &
               (data["amount"] >= CFG["min_amount"]) &
               (data["n_open"].notna())]
    cand = pre[pre["gap_ok"]].copy()
    log(f"候选信号总量：{len(cand):,} 条（BIAS<{BIAS_CAND}% 且成交额≥{CFG['min_amount']/1e8:.0f}千万）"
        f"｜剔除持有期内停牌 {len(pre)-len(cand):,} 条")

    cols = (["date", "code", "name", "close", "n_open", "bias", "vol_ok", "chg5d_ok", "idx_ok", "to_pct"] +
            [f"{p}{k}" for k in range(1, CFG["hold_max"] + 1) for p in ("L", "H", "C", "O", "bias_f")])
    recs = cand[cols].to_dict("records")
    log("构建价格路径 ...")
    paths = [p for p in (build_path(r) for r in recs) if p]
    log(f"有效路径：{len(paths):,} 条（已剔除次日涨停无法买入的）")

    all_rows = []
    for name, v in EXIT_VARIANTS.items():
        log(f"模拟方案 {name} ...")
        df = run_variant(paths, name, v)
        if df.empty:
            continue
        df["variant"] = name
        all_rows.append(df)
        df.to_csv(os.path.join(CFG["out_dir"], f"trades_{name.replace('/', '_')}.csv"),
                  index=False, encoding="utf-8-sig")
        log(f"  → {len(df):,} 笔  净均收益 {df['ret_net'].mean():+.2f}%  胜率 {(df['ret_net']>0).mean()*100:.1f}%")

    if not all_rows:
        log("没有产生任何交易")
        return
    allt = pd.concat(all_rows, ignore_index=True)
    allt.to_csv(os.path.join(CFG["out_dir"], "all_trades.csv"), index=False, encoding="utf-8-sig")

    # ---------------- 汇总表 ----------------
    rows = []

    # 1) 基线：BIAS<-15，放量确认开，5日跌为负
    base = allt[(allt["vol_ok"]) & (allt["chg5d_ok"])]
    for th in [-14, -15, -16]:
        for name in EXIT_VARIANTS:
            sub = base[(base["variant"] == name) & (base["bias"] <= th)]
            rows.append(dict(分析维度=f"①方案对比·BIAS<{th}·放量", 参数=name, **stats(sub)))

    # 2) 阈值网格（放量确认开）
    for name in EXIT_VARIANTS:
        for th in BIAS_GRID:
            sub = base[(base["variant"] == name) & (base["bias"] <= th)]
            rows.append(dict(分析维度="②阈值网格·放量", 参数=f"{name} | BIAS<{th}", **stats(sub)))

    # 3) 放量确认开关（阈值 -15）
    for name in EXIT_VARIANTS:
        for vol in [True, False]:
            sub = allt[(allt["variant"] == name) & (allt["bias"] <= -15) &
                       (allt["chg5d_ok"]) & (allt["vol_ok"] == vol)]
            rows.append(dict(分析维度="③放量开关·BIAS<-15", 参数=f"{name} | 放量={'开' if vol else '关'}",
                             **stats(sub)))

    # 4) 大盘环境开关（阈值 -15，放量开）
    for name in EXIT_VARIANTS:
        for idx in [True, False]:
            sub = allt[(allt["variant"] == name) & (allt["bias"] <= -15) &
                       (allt["chg5d_ok"]) & (allt["vol_ok"]) & (allt["idx_ok"] == idx)]
            rows.append(dict(分析维度="④大盘环境开关·BIAS<-15", 参数=f"{name} | 多头={'开' if idx else '关'}",
                             **stats(sub)))

    # 5) 换手率门槛（阈值 -15，放量开）—— 比较不同活跃度门槛
    for name in EXIT_VARIANTS:
        for to_th in [0, 1, 3, 5]:
            sub = allt[(allt["variant"] == name) & (allt["bias"] <= -15) &
                       (allt["chg5d_ok"]) & (allt["vol_ok"]) & (allt["to_pct"] >= to_th)]
            rows.append(dict(分析维度="⑤换手率门槛·BIAS<-15", 参数=f"{name} | 换手≥{to_th}%",
                             **stats(sub)))

    # 6) 全条件叠加最终对比（BIAS<-15，放量+换手≥1%+5日跌 全开）
    strict = allt[(allt["vol_ok"]) & (allt["chg5d_ok"]) & (allt["to_pct"] >= 1.0)]
    for name in EXIT_VARIANTS:
        for th in [-14, -15, -16, -18]:
            sub = strict[(strict["variant"] == name) & (strict["bias"] <= th)]
            rows.append(dict(分析维度="⑥全条件叠加·最终对比", 参数=f"{name} | BIAS<{th}", **stats(sub)))

    rpt = pd.DataFrame(rows)

    # 7) 出场原因分布（BIAS<-15，全条件叠加）
    reason_rows = []
    for name in EXIT_VARIANTS:
        sub = strict[(strict["variant"] == name) & (strict["bias"] <= -15)]
        if sub.empty:
            continue
        rsn = sub["reason"].str.replace(r"\(\d+日\)", "", regex=True).value_counts()
        for k_, v_ in rsn.items():
            reason_rows.append(dict(方案=name, 出场原因=k_, 笔数=int(v_),
                                    占比=round(v_ / len(sub) * 100, 1)))
    reason_df = pd.DataFrame(reason_rows)
    reason_df.to_csv(os.path.join(CFG["out_dir"], "出场原因分布.csv"), index=False, encoding="utf-8-sig")

    # 8) 分年度 / 分月度表现（基线：BIAS<-15 + 放量 + 5日跌）★ 检验收益是否可稳定复制
    per_rows = []
    for name in EXIT_VARIANTS:
        sub = base[(base["variant"] == name) & (base["bias"] <= -15)].copy()
        if sub.empty:
            continue
        sub["yr"] = pd.to_datetime(sub["date"]).dt.year
        sub["ym"] = pd.to_datetime(sub["date"]).dt.to_period("M").astype(str)
        for yr, g in sub.groupby("yr"):
            per_rows.append(dict(方案=name, 粒度="年", 期间=str(yr), **stats(g)))
        for ym, g in sub.groupby("ym"):
            per_rows.append(dict(方案=name, 粒度="月", 期间=ym, **stats(g)))
    per_df = pd.DataFrame(per_rows)
    per_df.to_csv(os.path.join(CFG["out_dir"], "分期间表现.csv"), index=False, encoding="utf-8-sig")

    out_xlsx = os.path.join(CFG["out_dir"], "回测汇总.xlsx")
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as xw:
        rpt.to_excel(xw, sheet_name="汇总", index=False)
        reason_df.to_excel(xw, sheet_name="出场原因分布", index=False)
        per_df.to_excel(xw, sheet_name="分期间表现", index=False)
        allt.head(50000).to_excel(xw, sheet_name="逐笔明细(前5万)", index=False)

    pd.set_option("display.width", 220)
    pd.set_option("display.max_rows", 400)
    for dim in rpt["分析维度"].unique():
        print("\n" + "=" * 150)
        print(f"【{dim}】")
        print("=" * 150)
        print(rpt[rpt["分析维度"] == dim].drop(columns=["分析维度"]).to_string(index=False))

    print("\n" + "=" * 150)
    print("【⑦出场原因分布 · BIAS<-15 · 全条件叠加】")
    print("=" * 150)
    print(reason_df.to_string(index=False))

    print("\n" + "=" * 150)
    print("【⑧分年度表现 · BIAS<-15 · 放量（★检验收益是否可稳定复制）】")
    print("=" * 150)
    print(per_df[per_df["粒度"] == "年"].drop(columns=["粒度"]).to_string(index=False))

    log(f"报告已保存：{out_xlsx}")
    log(f"逐笔明细：{os.path.join(CFG['out_dir'], 'all_trades.csv')}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        sys.exit(1)
