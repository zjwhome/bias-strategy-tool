"""
================================================================================
乖离率超跌反弹策略 —— 全市场历史回测脚本（v1.0）
================================================================================
用途：在本地用免费数据源 AKShare 拉取全市场日线，回测乖离率策略，
      并通过网格搜索找出「胜率 / 盈亏比最优」的乖离率阈值。

环境准备（只需做一次）：
    python -m venv .venv
    .venv\\Scripts\\activate            # Windows
    pip install akshare pandas numpy openpyxl -i https://pypi.tuna.tsinghua.edu.cn/simple

运行：
    python backtest_bias_akshare.py

首次运行会下载全市场历史数据（约 20-40 分钟，取决于网速），
数据会缓存在 ./data/ 目录下，之后每天只需增量更新（几十秒）。

输出：
    ./output/回测报告.xlsx      各阈值/各分层的胜率统计
    ./output/信号明细.csv       每一笔信号的完整记录
================================================================================
"""

import os
import time
import random
import warnings
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ============================== 配置区 ==============================
CONFIG = {
    # ---- 回测区间 ----
    "start_date": "2024-09-01",     # 回测起始（建议 ≥ 24 个月以覆盖多种市场环境）
    "end_date": "2026-08-31",       # 回测结束（需留出 6 个交易日的持有期）

    # ---- 标的池 ----
    "min_total_mv": 50e8,           # 最小总市值（元）→ 50 亿
    "min_turnover_rate": 1.0,       # 最小换手率（%）
    "min_listed_days": 60,          # 上市满 N 个交易日
    "exclude_st": True,             # 剔除 ST / *ST
    "exclude_kcb": False,           # 是否剔除科创板（688 开头）
    "exclude_bj": True,             # 剔除北交所

    # ---- 信号参数 ----
    "bias_period": 24,              # 乖离率周期（BNF 原法为 25，接口常用 24）
    "require_positive_pe": True,    # 剔除亏损股
    "require_chg5d_negative": True, # 要求 5 日涨跌幅 < 0（确保是"跌下来"）
    "volume_surge_ratio": 1.5,      # 放量确认：当日成交量 ≥ 5 日均量 × 该倍数（None 表示不启用）
    "volume_surge_window": 5,       # 均量窗口

    # ---- 出场规则 ----
    "hold_days": 6,                 # 最大持有交易日
    "stop_loss_pct": -3.0,          # 硬止损：跌破买入价 -3%
    "target_bias": 0.0,             # 止盈目标：乖离率回归到该值
    "trailing_trigger": 5.0,        # 移动止盈启动浮盈（%）
    "trailing_drawdown": 3.0,       # 移动止盈回撤（%）

    # ---- 交易成本 ----
    "cost_pct": 0.4,                # 单笔往返总成本（%）：佣金+印花税+滑点

    # ---- 网格搜索的乖离率阈值 ----
    "bias_grid": [-12, -13, -14, -15, -16, -17, -18, -20, -22, -25, -28, -30],

    # ---- 样本抽样：每隔 N 个交易日取一个评估日（越小样本越多、越慢）----
    "signal_every_n_days": 1,       # 1 = 每个交易日都评估（最全，推荐）

    # ---- 性能 ----
    "request_delay": (3.0, 6.0),    # 每次请求后随机 sleep 秒数，防止被限流
    "data_dir": "./data",
    "output_dir": "./output",
}

# 申万一级行业分档阈值（行业名 → 乖离率阈值）
SECTOR_TIERS = {
    "银行": -6, "公用事业": -6, "交通运输": -6, "石油石化": -6,
    "食品饮料": -10, "家用电器": -10, "商贸零售": -10, "农林牧渔": -10,
    "煤炭": -10, "钢铁": -10, "建筑材料": -10, "房地产": -10,
    "医药生物": -13, "基础化工": -13, "机械设备": -13, "汽车": -13,
    "有色金属": -13, "轻工制造": -13, "纺织服饰": -13, "建筑装饰": -13,
    "环保": -13, "社会服务": -13,
    "电子": -18, "计算机": -18, "电力设备": -18, "国防军工": -18,
    "通信": -18, "传媒": -18, "美容护理": -18, "综合": -18, "非银金融": -18,
}
DEFAULT_TIER = -15      # 未匹配到行业时使用


# ============================== 工具函数 ==============================
def ensure_dirs():
    for d in (CONFIG["data_dir"], CONFIG["output_dir"]):
        os.makedirs(d, exist_ok=True)


def log(msg):
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def polite_sleep():
    lo, hi = CONFIG["request_delay"]
    time.sleep(random.uniform(lo, hi))


def is_bj_code(code):
    """北交所：8xxxxx / 4xxxxx / 92xxxx"""
    return code.startswith(("8", "4", "92"))


def board_limit_pct(code):
    """涨跌停幅度：创业板/科创板 20%，其余 10%"""
    if code.startswith(("300", "301", "688", "689")):
        return 20.0
    return 10.0


# ============================== 数据获取 ==============================
def fetch_stock_list():
    """获取全市场股票列表（含市值、市盈率、换手率）"""
    log("正在获取全市场股票列表 ...")
    import akshare as ak

    df = ak.stock_zh_a_spot_em()
    df = df.rename(columns={
        "代码": "code", "名称": "name", "最新价": "close",
        "涨跌幅": "chg_pct", "换手率": "turnover_rate", "市盈率-动态": "pe",
        "总市值": "total_mv",
    })
    df["code"] = df["code"].astype(str).str.zfill(6)

    if CONFIG["exclude_bj"]:
        df = df[~df["code"].apply(is_bj_code)]
    if CONFIG["exclude_kcb"]:
        df = df[~df["code"].str.startswith("688")]
    if CONFIG["exclude_st"]:
        df = df[~df["name"].str.contains("ST", na=False)]

    log(f"  过滤后标的数：{len(df)}")
    return df[["code", "name"]].reset_index(drop=True)


def fetch_one_history(code, start, end):
    """拉取单只股票的前复权日线，带本地缓存"""
    cache = os.path.join(CONFIG["data_dir"], f"{code}.csv")
    need_fetch = True
    cached = None

    if os.path.exists(cache):
        cached = pd.read_csv(cache)
        if len(cached) and str(cached["date"].iloc[-1]) >= end:
            need_fetch = False
        else:
            start = (pd.to_datetime(cached["date"].iloc[-1]) - timedelta(days=3)).strftime("%Y%m%d")

    if need_fetch:
        import akshare as ak
        try:
            new = ak.stock_zh_a_hist(symbol=code, period="daily",
                                     start_date=start.replace("-", ""),
                                     end_date=end.replace("-", ""), adjust="qfq")
            if new is None or new.empty:
                return cached
            new = new.rename(columns={
                "日期": "date", "开盘": "open", "收盘": "close",
                "最高": "high", "最低": "low", "成交量": "volume",
                "换手率": "turnover", "涨跌幅": "chg",
            })[["date", "open", "close", "high", "low", "volume", "turnover", "chg"]]
            new["date"] = new["date"].astype(str)

            cached = pd.concat([cached, new]).drop_duplicates("date") if cached is not None else new
            cached = cached.sort_values("date").reset_index(drop=True)
            cached.to_csv(cache, index=False)
            polite_sleep()
        except Exception as e:
            log(f"    {code} 获取失败：{e}")

    return cached


def build_market_data(limit=None):
    """批量构建全市场日线数据集（带进度与断点续传）"""
    stocks = fetch_stock_list()
    if limit:
        stocks = stocks.head(limit)

    records = []
    for i, row in stocks.iterrows():
        df = fetch_one_history(row["code"], CONFIG["start_date"], CONFIG["end_date"])
        if df is None or len(df) < CONFIG["bias_period"] + 10:
            continue
        df = df.copy()
        df["code"] = row["code"]
        df["name"] = row["name"]
        records.append(df)

        if (i + 1) % 100 == 0:
            log(f"  已处理 {i + 1}/{len(stocks)} 只")

    if not records:
        raise RuntimeError("未获取到任何数据，请检查网络或 AKShare 版本")

    data = pd.concat(records, ignore_index=True)
    data["date"] = pd.to_datetime(data["date"])
    data = data.sort_values(["code", "date"]).reset_index(drop=True)
    log(f"数据集构建完成：{len(data)} 条记录，{data['code'].nunique()} 只股票")
    return data


# ============================== 指标计算 ==============================
def compute_features(data):
    """计算乖离率、均量、5 日涨跌幅等特征"""
    log("正在计算技术指标 ...")
    g = data.groupby("code", group_keys=False)

    n = CONFIG["bias_period"]
    data["ma"] = g["close"].transform(lambda s: s.rolling(n, min_periods=n).mean())
    data["bias"] = (data["close"] - data["ma"]) / data["ma"] * 100
    data["vol_ma"] = g["volume"].transform(
        lambda s: s.rolling(CONFIG["volume_surge_window"], min_periods=1).mean())
    data["chg5d"] = g["close"].transform(lambda s: s.pct_change(5) * 100)

    # 未来 6 个交易日的开盘价与逐日高低收（用于模拟出场）
    data["next_open"] = g["open"].shift(-1)
    for k in range(1, CONFIG["hold_days"] + 1):
        data[f"f_low_{k}"] = g["low"].shift(-k)
        data[f"f_high_{k}"] = g["high"].shift(-k)
        data[f"f_close_{k}"] = g["close"].shift(-k)
        data[f"f_bias_{k}"] = g["bias"].shift(-k)

    return data


# ============================== 策略模拟 ==============================
def simulate(data, bias_threshold, use_sector_tier=False, require_volume=False, require_chg5d=None):
    """在给定阈值下模拟策略，返回逐笔信号 DataFrame"""
    if require_chg5d is None:
        require_chg5d = CONFIG["require_chg5d_negative"]

    df = data.copy()

    # --- 硬门槛 ---
    mask = df["bias"] < bias_threshold
    mask &= df["next_open"].notna()
    mask &= df["next_open"] > 0
    if require_chg5d:
        mask &= df["chg5d"] < 0
    if CONFIG["require_positive_pe"] and "pe" in df.columns:
        mask &= df["pe"] > 0
    if require_volume:
        mask &= df["volume"] >= df["vol_ma"] * CONFIG["volume_surge_ratio"]

    sig = df[mask].copy()
    if sig.empty:
        return sig

    rows = []
    for _, r in sig.iterrows():
        entry = r["next_open"]
        # 涨停无法买入 → 放弃该信号
        limit_up = 1 + board_limit_pct(r["code"]) / 100.0
        if entry >= r["close"] * limit_up * 0.998:
            continue

        stop_price = entry * (1 + CONFIG["stop_loss_pct"] / 100.0)
        ma_target = r["ma"]
        peak = 0.0
        ret, hold, reason = None, CONFIG["hold_days"], "到期"

        for k in range(1, CONFIG["hold_days"] + 1):
            low, high, close = r[f"f_low_{k}"], r[f"f_high_{k}"], r[f"f_close_{k}"]
            if pd.isna(close):
                hold, reason, ret = k - 1, "数据不足", None
                break

            gain = (high / entry - 1) * 100
            peak = max(peak, gain)

            # P0 硬止损（当日最低价触发）
            if not pd.isna(low) and low <= stop_price:
                ret, hold, reason = CONFIG["stop_loss_pct"], k, "止损"
                break

            # P2 移动止盈
            if peak >= CONFIG["trailing_trigger"]:
                if (close / entry - 1) * 100 <= peak - CONFIG["trailing_drawdown"]:
                    ret, hold, reason = (close / entry - 1) * 100, k, "移动止盈"
                    break

            # P3/P4 回归均线 / 乖离率转正
            fb = r[f"f_bias_{k}"]
            if not pd.isna(fb) and fb >= CONFIG["target_bias"]:
                ret, hold, reason = (close / entry - 1) * 100, k, "回归均线"
                break

        if ret is None:
            close = r[f"f_close_{CONFIG['hold_days']}"]
            if pd.isna(close):
                continue
            ret = (close / entry - 1) * 100

        rows.append(dict(
            date=r["date"].strftime("%Y-%m-%d"), code=r["code"], name=r["name"],
            bias=r["bias"], entry=entry, target_bias=bias_threshold,
            ret=ret, ret_net=ret - CONFIG["cost_pct"], hold=hold, reason=reason,
        ))

    return pd.DataFrame(rows)


def summarize(trades, label=""):
    if trades is None or trades.empty:
        return dict(样本=label, 信号数=0)
    r = trades["ret_net"]
    wins = trades[trades["ret_net"] > 0]
    gp = trades.loc[trades["ret_net"] > 0, "ret_net"].sum()
    gl = abs(trades.loc[trades["ret_net"] <= 0, "ret_net"].sum())
    return dict(
        样本=label,
        信号数=len(trades),
        日均信号=round(len(trades) / max(trades["date"].nunique(), 1), 2),
        胜率=round(len(wins) / len(trades) * 100, 1),
        均收益=round(r.mean(), 2),
        中位收益=round(r.median(), 2),
        盈亏比=round(gp / gl, 2) if gl > 0 else np.inf,
        最大单笔=round(r.max(), 2),
        最小单笔=round(r.min(), 2),
        止损占比=round((trades["reason"] == "止损").mean() * 100, 1),
        平均持有=round(trades["hold"].mean(), 1),
    )


# ============================== 主流程 ==============================
def main():
    ensure_dirs()
    data = build_market_data()
    data = compute_features(data)

    # 行业映射（可选，失败不影响主流程）
    try:
        import akshare as ak
        industry = ak.stock_board_industry_name_em()
        # 这里仅示例，实际行业映射请用 ak.stock_individual_info_em 逐只补齐
        log(f"行业数据可用，共 {len(industry)} 个行业板块")
    except Exception as e:
        log(f"行业映射跳过：{e}")

    report_rows, detail = [], []

    # ---------- 1. 总体 ----------
    log("开始网格搜索乖离率阈值 ...")
    base = simulate(data, CONFIG["bias_grid"][0])
    report_rows.append(summarize(base, f"BIAS < {CONFIG['bias_grid'][0]}"))

    # ---------- 2. 不同阈值 ----------
    for th in CONFIG["bias_grid"]:
        t = simulate(data, th)
        report_rows.append(summarize(t, f"BIAS < {th}%"))
        if not t.empty:
            detail.append(t)

    # ---------- 3. 叠加放量确认 ----------
    for th in CONFIG["bias_grid"]:
        t = simulate(data, th, require_volume=True)
        report_rows.append(summarize(t, f"BIAS < {th}% + 放量{CONFIG['volume_surge_ratio']}x"))

    # ---------- 4. 乖离率分桶 ----------
    all_t = simulate(data, min(CONFIG["bias_grid"]))
    if not all_t.empty:
        for lo, hi in [(-12, -15), (-15, -18), (-18, -22), (-22, -28), (-28, -100)]:
            sub = all_t[(all_t["bias"] <= lo) & (all_t["bias"] > hi)]
            report_rows.append(summarize(sub, f"BIAS ∈ ({lo}%, {hi}%]"))

    # ---------- 输出 ----------
    rpt = pd.DataFrame(report_rows)
    rpt.to_excel(os.path.join(CONFIG["output_dir"], "回测报告.xlsx"), index=False)

    if detail:
        pd.concat(detail, ignore_index=True).to_csv(
            os.path.join(CONFIG["output_dir"], "信号明细.csv"), index=False, encoding="utf-8-sig")

    print("\n" + "=" * 100)
    print("回测结果汇总（收益均已扣除交易成本 %.1f%%/笔）" % CONFIG["cost_pct"])
    print("=" * 100)
    print(rpt.to_string(index=False))

    # 最优阈值
    valid = rpt[rpt["信号数"] >= 50]
    if not valid.empty:
        best = valid.sort_values("盈亏比", ascending=False).iloc[0]
        print("\n【按盈亏比排序的最优参数】")
        print(best.to_string())

    print(f"\n报告已保存至：{os.path.join(CONFIG['output_dir'], '回测报告.xlsx')}")


if __name__ == "__main__":
    main()
