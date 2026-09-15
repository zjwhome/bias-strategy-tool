# -*- coding: utf-8 -*-
"""按不同时间窗口统计策略胜率（口径：推荐方案 I，扣费后）
严格对齐 analyze_bias.py 的口径：
  base = all_trades[(vol_ok) & (chg5d_ok) & (bias <= -15)]
  breadth = base[单一方案].groupby("date").size()
"""
import os
import pandas as pd

# 项目根目录（脚本所在目录），避免硬编码本机路径
BASE = os.path.dirname(os.path.abspath(__file__))
P = os.path.join(BASE, "output_long", "all_trades.csv")
OUT = os.path.join(BASE, "output_long", "胜率统计_多窗口.txt")

cols = ["date", "bias", "ret_net", "reason", "variant", "vol_ok", "chg5d_ok", "to_pct"]
df = pd.read_csv(P, usecols=cols)
df["date"] = pd.to_datetime(df["date"])

lines = []
def w(s=""):
    lines.append(str(s))

V = "I_收盘-6%·分层止盈·10日"

# 基线信号集（严格对齐 analyze_bias.py）
base = df[(df["vol_ok"]) & (df["chg5d_ok"]) & (df["bias"] <= -15)].copy()
# 广度：只用单一方案计数，避免重复
br = base[base["variant"] == V].groupby("date").size().rename("breadth")

I = base[base["variant"] == V].copy()
I["breadth"] = I["date"].map(br)
assert I["breadth"].max() <= 800, f"广度异常：{I['breadth'].max()}"
assert len(I) == 8303, f"基线条数异常：{len(I)}"

w("=" * 82)
w("乖离率超跌反弹策略 · 胜率统计")
w(f"口径：方案 I（收盘-6%止损 + 分层止盈 + 持有10日），已扣费 0.4%/笔")
w(f"样本：2021-01 ~ 2026-08，基线 {len(I)} 笔，信号日 {len(br)} 天")
w("=" * 82)

def stat(sub, label):
    if sub.empty:
        w(f"  {label:<28} 无样本")
        return
    n = len(sub)
    win = (sub["ret_net"] > 0).mean() * 100
    avg = sub["ret_net"].mean()
    med = sub["ret_net"].median()
    stop = sub["reason"].str.startswith("止损").mean() * 100
    w(f"  {label:<28} n={n:>5}  胜率 {win:>5.1f}%   均 {avg:>+6.2f}%   中位 {med:>+6.2f}%   止损率 {stop:>5.1f}%")

w("")
w("【A】六年全样本（2021-01 ~ 2026-08）")
stat(I, "不设广度门槛")
stat(I[I.breadth >= 20], "广度 ≥ 20")
stat(I[I.breadth >= 50], "★ 广度 ≥ 50（推荐口径）")
stat(I[I.breadth >= 100], "广度 ≥ 100")

w("")
w("【B】分年度 · 广度≥50（只看达标日）")
for y in range(2021, 2027):
    stat(I[(I.date.dt.year == y) & (I.breadth >= 50)], f"{y} 年")

w("")
w("【C】分年度 · 不设门槛（所有信号）")
for y in range(2021, 2027):
    stat(I[I.date.dt.year == y], f"{y} 年")

w("")
w("【D】滚动时间窗口（截至 2026-08-31）")
for m, lab in [(3, "最近 3 个月"), (6, "最近 6 个月"), (12, "最近 12 个月"), (24, "最近 24 个月")]:
    start = I.date.max() - pd.DateOffset(months=m)
    sub = I[I.date > start]
    stat(sub, f"{lab} · 全部信号")
    stat(sub[sub.breadth >= 50], f"{lab} · 达标日(≥50)")

w("")
w("【E】2026 年逐月")
cur = I[I.date.dt.year == 2026]
for ym, g in cur.groupby(cur.date.dt.to_period("M")):
    stat(g, f"{ym}")

w("")
w("【F】2026 年达标日（广度≥50）逐日")
for d, g in cur[cur.breadth >= 50].groupby("date"):
    w(f"    {d.date()}  广度{int(g.breadth.iloc[0]):>5}  笔数{len(g):>4}  胜率{(g.ret_net>0).mean()*100:>6.1f}%  均{g.ret_net.mean():>+7.2f}%")

with open(OUT, "w", encoding="utf-8") as f:
    f.write("\n".join(lines))
print("done, lines =", len(lines))
