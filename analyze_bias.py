# -*- coding: utf-8 -*-
"""
================================================================================
乖离率策略 —— 广度择时 & 稳健性分析（通用版）
================================================================================
用法：
    python analyze_bias.py <output_dir> [标签]
例：
    python analyze_bias.py ./output_full  "2年全市场"
    python analyze_bias.py ./output_long  "6年全市场"

分析内容：
    1. 基线表现（BIAS<-15 + 放量 + 5日跌）
    2. ★ 信号广度分桶 —— 检验「市场级恐慌」是否为真正的收益来源
    3. ★ 剔除最大事件月后的表现 —— 检验收益是否依赖单一事件
    4. ★ 分年度表现
    5. 广度门槛过滤效果（0/20/50/100）
    6. 阈值网格
    7. 信号机会密度（广度≥20 的交易日清单）
结果写入  <output_dir>/分析_<标签>.txt
================================================================================
"""
import os
import sys

import numpy as np
import pandas as pd

pd.set_option("display.width", 230)
pd.set_option("display.max_rows", 500)

OUTDIR = sys.argv[1] if len(sys.argv) > 1 else "./output_full"
LABEL = sys.argv[2] if len(sys.argv) > 2 else os.path.basename(os.path.abspath(OUTDIR))
LOG = open(os.path.join(OUTDIR, f"分析_{LABEL}.txt"), "w", encoding="utf-8")


def w(s=""):
    LOG.write(str(s) + "\n")
    LOG.flush()


def agg(df):
    """单组统计：n / 均收益 / 截尾均收益 / 中位 / 胜率 / 盈亏比"""
    if df is None or len(df) == 0:
        return dict(n=0, 均收益=np.nan, 中位=np.nan, 胜率=np.nan, 盈亏比=np.nan)
    r = df["ret_net"]
    gp, gl = r[r > 0].sum(), abs(r[r <= 0].sum())
    return dict(n=len(df), 均收益=round(r.mean(), 2), 截尾=round(r.clip(-20, 20).mean(), 2),
                中位=round(r.median(), 2), 胜率=round((r > 0).mean() * 100, 1),
                盈亏比=round(gp / gl, 2) if gl > 0 else 99.0)


def line(name, a):
    return (f"{name:36s} n={a['n']:5d}  均{a['均收益']:+6.2f}%  截尾{a['截尾']:+6.2f}%  "
            f"中位{a['中位']:+6.2f}%  胜率{a['胜率']:5.1f}%  盈亏比{a['盈亏比']:6.2f}")


def main():
    t = pd.read_csv(os.path.join(OUTDIR, "all_trades.csv"))
    t["dt"] = pd.to_datetime(t["date"])
    t["ym"] = t["dt"].dt.to_period("M").astype(str)
    t["yr"] = t["dt"].dt.year

    base = t[(t["vol_ok"]) & (t["chg5d_ok"]) & (t["bias"] <= -15)].copy()
    VARIANTS = sorted(base["variant"].unique())

    # ★ 信号广度：当日全市场符合基线条件的股票数（实盘可实时观测）
    br = base[base["variant"] == VARIANTS[0]].groupby("date").size()
    base["breadth"] = base["date"].map(br)

    w("=" * 118)
    w(f"乖离率策略 · 广度择时与稳健性分析    数据集：{LABEL}")
    w(f"标的池基准：BIAS_24 < -15% 且 放量≥1.5× 且 5日跌幅<0 ｜ 逐笔样本 {len(base)} 条")
    w("=" * 118)

    # ---------------- 1. 基线 ----------------
    w("\n【1】各出场方案基线表现（全样本）")
    w("-" * 118)
    for v in VARIANTS:
        w(line(v, agg(base[base["variant"] == v])))

    # ---------------- 2. 广度分桶 ----------------
    w("\n\n【2】★ 按「信号广度」分桶 —— 广度大 = 市场级系统性暴跌")
    w("-" * 118)
    w(f"信号日数 {len(br)}；广度分布：")
    w(br.describe(percentiles=[.5, .75, .9, .95, .99]).round(2).to_string())
    w("\n广度最高的 12 个交易日：")
    w(br.sort_values(ascending=False).head(12).rename("信号数").to_frame().to_string())

    bins, labels = [0, 5, 20, 50, 100, 300, 10 ** 6], ["1-5", "6-20", "21-50", "51-100", "101-300", ">300"]
    for v in VARIANTS:
        s = base[base["variant"] == v].copy()
        s["桶"] = pd.cut(s["breadth"], bins=bins, labels=labels)
        g = s.groupby("桶", observed=True)["ret_net"].agg(
            信号数="count", 均收益="mean", 中位="median",
            胜率=lambda x: (x > 0).mean() * 100,
            盈亏比=lambda x: x[x > 0].sum() / abs(x[x <= 0].sum()) if (x <= 0).any() else 99)
        w(f"\n  {v}")
        w(g.round(2).to_string())

    # ---------------- 3. 剔除最大事件月 ----------------
    w("\n\n【3】★ 剔除信号最集中的月份后，收益还剩多少（检验是否依赖单一事件）")
    w("-" * 118)
    lead = base[base["variant"] == VARIANTS[0]].groupby("ym").size().sort_values(ascending=False)
    top_months = list(lead.head(3).index)
    w(f"信号最多的 3 个月：{top_months}（各 {list(lead.head(3).values)} 条，"
      f"合计占 {lead.head(3).sum()/len(base[base['variant']==VARIANTS[0]])*100:.1f}%）\n")
    ex = base[~base["ym"].isin(top_months)]
    for v in VARIANTS:
        w(f"  {v}")
        w(f"      全样本      {line('', agg(base[base['variant'] == v])).strip()}")
        w(f"      剔除前3月   {line('', agg(ex[ex['variant'] == v])).strip()}")

    # ---------------- 4. 分年度 ----------------
    w("\n\n【4】★ 分年度表现")
    w("-" * 118)
    for v in VARIANTS:
        w(f"\n  {v}")
        yrs = base[base["variant"] == v].groupby("yr")["ret_net"].agg(
            信号数="count", 均收益="mean", 中位="median",
            胜率=lambda x: (x > 0).mean() * 100)
        w(yrs.round(2).to_string())

    # ---------------- 5. 广度门槛 ----------------
    w("\n\n【5】★ 广度门槛过滤效果（只在广度达标的「恐慌日」进场）")
    w("-" * 118)
    for th in [0, 20, 50, 100]:
        w(f"\n  ── 广度 ≥ {th} ──")
        for v in VARIANTS:
            s = base[(base["variant"] == v) & (base["breadth"] >= th)]
            w("    " + line(v, agg(s)))
        # 剔除最大事件月后的样本外检验
        s2 = base[(base["variant"] == VARIANTS[0]) & (base["breadth"] >= th)]
        if len(s2):
            w(f"    (该门槛下单信号日数={s2['date'].nunique()})")

    # ---------------- 6. 阈值网格 ----------------
    w("\n\n【6】阈值网格（广度≥20 条件下）")
    w("-" * 118)
    for v in VARIANTS:
        w(f"\n  {v}")
        for th in [-12, -14, -15, -16, -18, -20, -22]:
            s = base[(base["variant"] == v) & (base["breadth"] >= 20) & (base["bias"] <= th)]
            w("    " + line(f"BIAS<{th}", agg(s)))

    # ---------------- 7. 机会密度 ----------------
    w("\n\n【7】机会密度：广度≥20 的交易日清单（实盘等待成本）")
    w("-" * 118)
    days = base[(base["variant"] == VARIANTS[0]) & (base["breadth"] >= 20)] \
        .groupby("date").size().rename("信号数").reset_index()
    n_all = base[base["variant"] == VARIANTS[0]]["date"].nunique()
    w(f"共 {len(days)} 个交易日出现广度≥20（占全部 {n_all} 个信号日的 {len(days)/n_all*100:.1f}%）")
    w(days.to_string(index=False))

    # ---------------- 8. 月度明细（做趋势图用） ----------------
    w("\n\n【8】月度明细（基线全样本，最佳方案 I：收盘-6%+分层止盈）")
    w("-" * 118)
    pick = [v for v in VARIANTS if v.startswith("I_")] or [VARIANTS[0]]
    mv = pick[0]
    mo = base[base["variant"] == mv].groupby("ym")["ret_net"].agg(
        信号数="count", 均收益="mean", 中位="median",
        胜率=lambda x: (x > 0).mean() * 100).round(2)
    mo["累计收益"] = (mo["均收益"] * mo["信号数"]).cumsum().round(0)
    w(f"方案：{mv}")
    w(mo.to_string())
    mo.to_csv(os.path.join(OUTDIR, f"月度明细_{LABEL}.csv"), encoding="utf-8-sig")

    # 导出广度分桶表（做图用）
    s = base[base["variant"] == mv].copy()
    s["桶"] = pd.cut(s["breadth"], bins=bins, labels=labels)
    bt_ = s.groupby("桶", observed=True)["ret_net"].agg(
        信号数="count", 均收益="mean", 中位="median",
        胜率=lambda x: (x > 0).mean() * 100).round(2)
    bt_.to_csv(os.path.join(OUTDIR, f"广度分桶_{LABEL}.csv"), encoding="utf-8-sig")
    w(f"\n广度分桶表已导出：广度分桶_{LABEL}.csv")

    LOG.close()
    print(f"分析完成 → {os.path.join(OUTDIR, f'分析_{LABEL}.txt')}")


if __name__ == "__main__":
    main()
