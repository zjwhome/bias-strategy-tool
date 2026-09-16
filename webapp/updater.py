# -*- coding: utf-8 -*-
"""
================================================================================
乖离率策略 · 数据更新器 (updater.py)
================================================================================
既可作为命令行工具，也可被任务系统（tasks.py）调用。

命令行用法：
    python updater.py                    完整流程：更新数据 → 算广度 → 存库
    python updater.py --no-fetch         跳过下载，只用本地缓存计算
    python updater.py --limit 300        只处理前 300 只（测试用）
    python updater.py --date 2026-09-14  指定交易日（回看用）

作为模块调用：
    import updater
    res = updater.run_update(no_fetch=True, log=print)
================================================================================
"""
from __future__ import annotations

import argparse
import os
import sys
import time

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import strategy_core as core
import db

BANNER = "=" * 68


def run_update(limit: int = 0, date: str = "", no_fetch: bool = False,
               force_universe: bool = False, workers: int = 0,
               log=None, write_history: bool = True, progress=None) -> dict:
    """执行一次全市场更新 + 广度计算。返回结构化结果 dict。

    limit      只处理前 N 只（测试用，0 = 全部）
    date       指定交易日 YYYY-MM-DD（回看用）
    no_fetch   跳过网络下载，只用本地缓存
    workers    并发线程数（0 = 用配置默认值）
    log        日志回调，默认 print
    progress   抓取进度回调 progress(done, total, ok, fail)
    """
    log = log or (lambda s: print(s, flush=True))
    t0 = time.time()
    db.init_db()

    # ---------- 1. 股票清单 ----------
    uni = core.load_universe(force=force_universe)
    log(f"[1/5] 股票清单：{len(uni)} 只")

    codes = uni["code"].tolist()
    partial = bool(limit)          # ★ 测试模式标记：只跑了一部分股票 → 结果不入库
    if limit:
        codes = codes[:limit]
        log(f"      ⚠️ 测试模式：只处理前 {len(codes)} 只（结果不写入数据库）")

    # ---------- 2. 更新数据 ----------
    ok = fail = 0
    if no_fetch:
        log("[2/5] 跳过数据下载（no_fetch）")
    else:
        log(f"[2/5] 更新全市场日线（{workers or core.CFG['workers']} 线程）…")
        ok, fail = core.update_cache(codes, workers=workers or None, progress=progress)
        log(f"      下载完成：成功 {ok}，失败 {fail}，耗时 {(time.time()-t0)/60:.1f} 分钟")

    # ---------- 3. 汇总数据集 ----------
    log("[3/5] 汇总本地数据并计算指标…")
    data = core.load_dataset(codes=codes)
    data = core.compute_features(data)
    data = data.merge(uni[["code", "name"]], on="code", how="left")

    # ---------- 4. 算广度 ----------
    target = pd.Timestamp(date) if date else core.latest_trade_date(data)
    snap = core.calc_breadth(data, target)
    log(f"[4/5] {snap['date']} 信号广度 = {snap['breadth']} 只"
        f"（仅乖离率≤{core.CFG['bias_threshold']}%：{snap['bias_only']} 只，"
        f"门槛 {snap['threshold']}）")
    log(f"      其中沪深主板：广度 {snap['breadth_main']} 只、"
        f"仅乖离率 {snap['bias_only_main']} 只")

    # ---------- 5. 候选股 ----------
    # ★ 用户只看沪深主板 → 买入名单按主板过滤（全市场清单可用 board_only=False 拿到）
    cand = core.pick_candidates(data, target, limit=10)
    n_sig_all = len(core.signal_rows(data, target, board_only=False))
    n_sig_mb = len(core.signal_rows(data, target, board_only=True))
    rows = cand.to_dict("records") if not cand.empty else []
    if core.CFG.get("main_board_only"):
        log(f"[候选] 达标 {n_sig_all} 只 → 主板 {n_sig_mb} 只 → 取前 {len(rows)} 只")

    snap = dict(snap)
    snap.update(fetch_ok=ok, fetch_fail=fail, candidates=rows,
                elapsed_min=round((time.time() - t0) / 60, 1))

    # ★ 写库前的健全性检查 —— 宁可什么都不写，也不要往库里塞错误数据。
    #   ① 指定日期不是交易日 / 数据缺失 → 全市场 0 只，写进去会变成"假广度 0"，
    #      并且在曲线上留下一个错误的空档（历史 bug 就源于此）。
    #   ② 测试模式（--limit）只跑了部分股票 → 广度必然偏低，不代表全市场。
    if snap["stocks_total"] == 0:
        log(f"      ⚠️ {snap['date']} 无任何行情数据（可能不是交易日），已跳过写库。")
        return snap
    if partial:
        log(f"      ⚠️ 测试模式（仅 {len(codes)} 只股票），结果不写入数据库。")
        return snap

    log(f"[5/5] 候选股 {len(rows)} 只")
    db.save_daily(snap)
    db.save_candidates(snap["date"], rows)

    # ---------- 广度历史（近 250 日，供前端画曲线） ----------
    # 历史行始终覆盖写入；当日跳过（当日快照信息更完整）
    if write_history:
        try:
            hist = core.breadth_history(data, days=250)
            for _, r in hist.iterrows():
                d0 = str(pd.Timestamp(r["date"]).date())
                if d0 == snap["date"]:
                    continue
                db.save_daily(dict(date=d0, breadth=int(r["breadth"]),
                                   threshold=core.CFG["breadth_threshold"],
                                   triggered=int(r["breadth"]) >= core.CFG["breadth_threshold"],
                                   bias_only=int(r["bias_only"]),
                                   breadth_main=int(r.get("breadth_main", 0)),
                                   bias_only_main=int(r.get("bias_only_main", 0)),
                                   stocks_total=int(r["total"])))
            log(f"      广度历史已写入（近 {len(hist)} 个交易日）")
        except Exception as e:
            log(f"      [warn] 历史广度写入失败：{e}")

    return snap


# ------------------------------------------------------------------ 命令行
def _banner(s: str) -> None:
    print("\n" + BANNER)
    print(s)
    print(BANNER, flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(description="乖离率策略每日更新")
    ap.add_argument("--limit", type=int, default=0, help="只处理前 N 只（测试用）")
    ap.add_argument("--date", type=str, default="", help="指定交易日 YYYY-MM-DD")
    ap.add_argument("--no-fetch", action="store_true", help="跳过下载，只用本地缓存")
    ap.add_argument("--force-universe", action="store_true", help="强制刷新股票清单")
    ap.add_argument("--workers", type=int, default=0, help="并发线程数")
    args = ap.parse_args()

    _banner("乖离率策略 · 每日更新")
    print(f"参数：门槛 广度≥{core.CFG['breadth_threshold']} | "
          f"BIAS≤{core.CFG['bias_threshold']}% | 放量≥{core.CFG['vol_surge']}× | "
          f"成交额≥{core.CFG['min_amount']/1e4:.0f}万元\n")

    res = run_update(limit=args.limit, date=args.date, no_fetch=args.no_fetch,
                     force_universe=args.force_universe, workers=args.workers)

    _banner("结果")
    print(f"      交易日　　：{res['date']}")
    print(f"      全市场股票：{res['stocks_total']:,} 只")
    print(f"      仅乖离率　：{res['bias_only']:,} 只（参考）")
    print(f"      ★ 信号广度：{res['breadth']:,} 只")
    print(f"      门槛　　　：≥ {res['threshold']}")
    print(f"      判定　　　：{'✅ 达标 —— 可以出手' if res['triggered'] else '⛔ 未达标 —— 今天不动手'}")
    if res["candidates"]:
        print("\n      候选股（按 BIAS 升序）：")
        show = pd.DataFrame(res["candidates"])
        for c in ("bias", "turnover_pct", "vol_ratio", "chg5d"):
            if c in show:
                show[c] = show[c].round(2)
        print(show.to_string(index=False))
    print(f"\n总耗时 {res['elapsed_min']} 分钟　|　数据库：{db.DB_PATH}")
    print(BANNER)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
