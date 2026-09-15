# -*- coding: utf-8 -*-
"""最终阈值选择：广度门槛 × 剔除最大事件月 的交叉检验"""
import os
import numpy as np
import pandas as pd

OUT = "output_long"
LOG = open(os.path.join(OUT, "阈值选择_6年.txt"), "w", encoding="utf-8")
def w(s=""):
    LOG.write(str(s) + "\n"); LOG.flush()

t = pd.read_csv(os.path.join(OUT, "all_trades.csv"))
t["dt"] = pd.to_datetime(t["date"])
t["ym"] = t["dt"].dt.to_period("M").astype(str)

base = t[(t["vol_ok"]) & (t["chg5d_ok"]) & (t["bias"] <= -15)].copy()
V0 = sorted(base["variant"].unique())[0]
br = base[base["variant"] == V0].groupby("date").size()
base["breadth"] = base["date"].map(br)

def agg(df):
    if len(df) == 0:
        return "n=0"
    r = df["ret_net"]
    gl = abs(r[r <= 0].sum())
    return (f"n={len(df):5d}  均{r.mean():+6.2f}%  中位{r.median():+6.2f}%  "
            f"胜率{(r>0).mean()*100:5.1f}%  盈亏比{r[r>0].sum()/gl if gl>0 else 99:6.2f}  "
            f"日数{df['date'].nunique():3d}")

VARS = ["D_收盘-4%·分层止盈·10日", "I_收盘-6%·分层止盈·10日", "J_收盘-4%·不止盈·10日(基准)"]
TOPM = ["2025-04", "2024-02", "2022-04"]

w("=" * 122)
w("广度门槛选择 · 交叉检验（数据集：2021-01 ~ 2026-08，4993 只沪深A股）")
w("=" * 122)
w("说明：'剔三大事件月' = 剔除 2025-04 / 2024-02 / 2022-04（合计占 44.4% 信号）")
w("      这一列衡量的是：把最有利的几次崩盘反弹拿掉后，策略还剩多少 —— 越稳越好")

for v in VARS:
    w(f"\n{'─'*122}\n【{v}】")
    w(f"{'广度门槛':<10}{'口径':<16}{'统计'}")
    for th in [0, 20, 30, 50, 80, 100, 150]:
        s = base[(base["variant"] == v) & (base["breadth"] >= th)]
        w(f"  ≥{th:<8}{'全样本':<16}{agg(s)}")
        w(f"  {'':<8}{'剔三大事件月':<16}{agg(s[~s['ym'].isin(TOPM)])}")

# 只保留广度≥门槛、且剔除三大事件月后，按年度看
w("\n" + "=" * 122)
w("广度≥50 且剔除三大事件月后，分年度表现（最严格的稳健性检验）")
w("=" * 122)
for v in VARS:
    s = base[(base["variant"] == v) & (base["breadth"] >= 50) & (~base["ym"].isin(TOPM))]
    w(f"\n  {v}  总计 {agg(s)}")
    if len(s):
        g = s.groupby(s["dt"].dt.year)["ret_net"].agg(
            信号数="count", 均收益="mean", 中位="median",
            胜率=lambda x: (x > 0).mean() * 100).round(2)
        g["信号日数"] = s.groupby(s["dt"].dt.year)["date"].nunique()
        w(g.to_string())

# 广度>=50 触发日清单（看事件多样性）
w("\n" + "=" * 122)
w("广度≥50 的交易日清单（事件多样性）")
w("=" * 122)
d = base[(base["variant"] == V0) & (base["breadth"] >= 50)].groupby("date").size().rename("信号数").reset_index()
w(d.to_string(index=False))
LOG.close()
print("阈值选择分析完成")
