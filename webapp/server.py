# -*- coding: utf-8 -*-
"""
================================================================================
乖离率策略 · 本地工具站后端 (server.py)
================================================================================
启动：  python server.py          （或在 控制台.bat 菜单里选「启动工具网站」）
访问：  http://127.0.0.1:8000

★ 任务调度原则：**网站绝不自己定时。**
   三个任务（盘前/盘中/盘后）默认全部关闭；只有在网页上被用户显式开启「定时执行」后，
   内置调度器才会按设定时间触发。任何时候用户都可以关掉或改成「立即执行」。

接口一览：
  GET    /                          首页看板
  GET    /api/status                数据新鲜度 + 服务状态
  GET    /api/today                 今日广度 + 候选股 + 该不该动手
  GET    /api/history?days=250      历史广度序列
  GET    /api/holdings              我的持仓（含实时盈亏 + 卖出提示）
  POST   /api/holdings              新增持仓
  POST   /api/holdings/<id>/close   平仓
  DELETE /api/holdings/<id>         删除持仓记录
  GET    /api/tasks                 三个任务的设置 + 运行态 + 最近执行
  POST   /api/tasks/<key>/run       立即执行任务
  POST   /api/tasks/<key>/schedule  设置/取消定时（{enabled, at_time, days}）
  GET    /api/tasks/<key>/runs      执行历史（含结果明细）
================================================================================
"""
from __future__ import annotations

import json
import os
import sys
import threading
import traceback
from datetime import datetime

from flask import Flask, jsonify, request, send_from_directory

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import strategy_core as core
import db
import tasks

WEBUI = os.path.join(HERE, "webui")
app = Flask(__name__, static_folder=None)

# 后台"更新数据"按钮的运行态
UPDATE_STATE = {"running": False, "last": None, "msg": "尚未更新"}


# ------------------------------------------------------------------ 静态页面
@app.route("/")
def index():
    return send_from_directory(WEBUI, "index.html")


@app.route("/<path:fname>")
def static_files(fname):
    return send_from_directory(WEBUI, fname)


# ------------------------------------------------------------------ 状态
@app.get("/api/status")
def api_status():
    d = db.get_daily()
    running = [k for k in tasks.TASK_KEYS if tasks.STATE[k]["running"]]
    return jsonify(dict(
        ok=True,
        last_date=d["date"] if d else None,
        updated_at=d["updated_at"] if d else None,
        breadth=d["breadth"] if d else None,
        threshold=core.CFG["breadth_threshold"],
        updating=UPDATE_STATE["running"],
        update_msg=UPDATE_STATE["msg"],
        running_tasks=running,
        server_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    ))


@app.get("/api/today")
def api_today():
    d = db.get_daily()
    if not d:
        return jsonify(dict(ok=False, msg="尚无数据，请先到「任务中心」跑一次「盘后任务」。"))
    cands = db.get_candidates(d["date"])
    return jsonify(dict(
        ok=True,
        date=d["date"],
        breadth=d["breadth"],
        bias_only=d["bias_only"],
        stocks_total=d["stocks_total"],
        threshold=d["threshold"],
        triggered=bool(d["triggered"]),
        updated_at=d["updated_at"],
        action=("✅ 达标：可以按下方候选股出手" if d["triggered"]
                else f"⛔ 未达标：今天不动手（需广度 ≥ {d['threshold']}）"),
        candidates=cands,
    ))


@app.get("/api/history")
def api_history():
    days = int(request.args.get("days", 250))
    rows = db.get_daily_history(days)
    return jsonify(dict(ok=True, threshold=core.CFG["breadth_threshold"],
                        rows=[dict(date=r["date"], breadth=r["breadth"]) for r in rows]))


# ------------------------------------------------------------------ 持仓
@app.get("/api/holdings")
def api_holdings():
    d = db.get_daily()
    hs = tasks.evaluate_holdings(latest_market_date=d["date"] if d else "")
    return jsonify(dict(ok=True, holdings=hs))


@app.post("/api/holdings")
def api_add_holding():
    j = request.get_json(force=True, silent=True) or {}
    # 注意用 `is None` / 空串判断，而不是 `not j.get(k)`：
    # 否则 buy_price=0 会被误报成「缺少字段」，掩盖真正的原因
    for k in ("code", "buy_date", "buy_price"):
        if j.get(k) is None or j.get(k) == "":
            return jsonify(dict(ok=False, msg=f"缺少字段 {k}")), 400

    # ★ 校验代码：必须是 6 位数字，否则后续行情查询全部落空（持仓永远无法估值）
    code = str(j["code"]).strip().zfill(6)
    if not (len(code) == 6 and code.isdigit()):
        return jsonify(dict(ok=False, msg="股票代码必须是 6 位数字，例如 300274")), 400

    # ★ 校验买入价：<=0 会让止损线/止盈线全部变成 0，
    #   页面会立刻误报「已达 +10% 清仓」，必须在入口挡住
    try:
        buy_price = float(j["buy_price"])
    except (TypeError, ValueError):
        return jsonify(dict(ok=False, msg="买入价格式不正确")), 400
    if buy_price <= 0:
        return jsonify(dict(ok=False, msg="买入价必须大于 0")), 400

    # ★ 校验日期格式，避免脏数据导致持有天数计算异常
    buy_date = str(j["buy_date"]).strip()
    try:
        datetime.strptime(buy_date, "%Y-%m-%d")
    except ValueError:
        return jsonify(dict(ok=False, msg="买入日期格式应为 YYYY-MM-DD")), 400

    try:
        shares = int(j.get("shares") or 0)
    except (TypeError, ValueError):
        shares = 0
    if shares < 0:
        shares = 0

    name = j.get("name") or core.load_names().get(code, "")
    hid = db.add_holding(code, name, buy_date, buy_price, shares, j.get("note", ""))
    return jsonify(dict(ok=True, id=hid))


@app.post("/api/holdings/<int:hid>/close")
def api_close_holding(hid):
    j = request.get_json(force=True, silent=True) or {}
    try:
        sp = float(j.get("sell_price") or 0)
    except (TypeError, ValueError):
        return jsonify(dict(ok=False, msg="卖出价格式不正确")), 400
    if sp <= 0:
        return jsonify(dict(ok=False, msg="卖出价必须大于 0")), 400
    sd = str(j.get("sell_date") or datetime.now().strftime("%Y-%m-%d")).strip()
    try:
        datetime.strptime(sd, "%Y-%m-%d")
    except ValueError:
        return jsonify(dict(ok=False, msg="卖出日期格式应为 YYYY-MM-DD")), 400
    db.close_holding(hid, sd, sp, j.get("reason", "手动平仓"))
    return jsonify(dict(ok=True))


@app.delete("/api/holdings/<int:hid>")
def api_delete_holding(hid):
    db.delete_holding(hid)
    return jsonify(dict(ok=True))


# ------------------------------------------------------------------ 任务中心
@app.get("/api/tasks")
def api_tasks():
    return jsonify(dict(ok=True, tasks=tasks.get_task_overview(),
                        scheduler=SCHEDULER_STATUS,
                        server_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")))


@app.post("/api/tasks/<key>/run")
def api_task_run(key):
    if key not in tasks.TASK_KEYS:
        return jsonify(dict(ok=False, msg="未知任务")), 404
    busy = [k for k in tasks.TASK_KEYS if tasks.STATE[k]["running"]]
    if not busy:
        # ★ 跨进程检查：控制台窗口可能正在跑任务，它的状态在另一个进程里
        try:
            busy = db.has_running_task()
        except Exception:
            busy = []
    if busy:
        return jsonify(dict(ok=False, msg=f"已有任务在执行：{','.join(busy)}，请等它跑完。"))
    if not tasks.run_task_async(key, "manual"):
        return jsonify(dict(ok=False, msg="该任务正在执行中"))
    return jsonify(dict(ok=True, msg=f"已开始执行：{tasks.TASK_DEFS[key]['name']}"))


@app.post("/api/tasks/<key>/schedule")
def api_task_schedule(key):
    if key not in tasks.TASK_KEYS:
        return jsonify(dict(ok=False, msg="未知任务")), 404
    j = request.get_json(force=True, silent=True) or {}
    enabled = bool(j.get("enabled"))
    at_time = (j.get("at_time") or tasks.TASK_DEFS[key]["suggest"]).strip()
    days = (j.get("days") or "1,2,3,4,5").strip()
    # 校验时间格式
    try:
        hh, mm = [int(x) for x in at_time.split(":")]
        assert 0 <= hh <= 23 and 0 <= mm <= 59
    except Exception:
        return jsonify(dict(ok=False, msg="时间格式应为 HH:MM，例如 15:35")), 400
    db.set_task_setting(key, enabled, f"{hh:02d}:{mm:02d}", days)
    return jsonify(dict(ok=True, enabled=enabled, at_time=f"{hh:02d}:{mm:02d}", days=days,
                        msg=("定时已开启" if enabled else "定时已关闭")))


@app.get("/api/tasks/<key>/runs")
def api_task_runs(key):
    limit = int(request.args.get("limit", 20))
    rows = db.get_task_runs(key, limit=limit)
    rows.reverse()                       # 旧 → 新，方便前端展示
    return jsonify(dict(ok=True, runs=rows))


@app.get("/api/tasks/<key>/result/<int:run_id>")
def api_task_result(key, run_id):
    rows = [r for r in db.get_task_runs(key, limit=200) if r["id"] == run_id]
    if not rows:
        return jsonify(dict(ok=False, msg="找不到该次执行记录")), 404
    r = rows[0]
    summary = None
    if r.get("summary"):
        try:
            summary = json.loads(r["summary"])
        except Exception:
            summary = None
    return jsonify(dict(ok=True, run=r, result=summary))


# ------------------------------------------------------------------ 兼容：网站内「更新数据」按钮
@app.post("/api/update")
def api_update():
    """等价于执行一次「盘后任务」（用户手动触发）"""
    if UPDATE_STATE["running"] or any(tasks.STATE[k]["running"] for k in tasks.TASK_KEYS):
        return jsonify(dict(ok=False, msg="已有任务在运行中"))
    no_fetch = request.args.get("no_fetch", "0") in ("1", "true", "yes")
    limit = int(request.args.get("limit", 0) or 0)

    def work():
        UPDATE_STATE.update(running=True, msg="正在更新数据…")
        try:
            tasks.run_task("postmarket", "manual",
                           log=lambda s: UPDATE_STATE.update(msg=str(s)))
            UPDATE_STATE.update(msg="更新完成",
                               last=datetime.now().isoformat(timespec="seconds"))
        except Exception as e:
            traceback.print_exc()
            UPDATE_STATE.update(msg=f"更新失败：{e}")
        finally:
            UPDATE_STATE["running"] = False

    threading.Thread(target=work, daemon=True).start()
    return jsonify(dict(ok=True, msg="已开始后台更新"))


# ================================================================== 内置调度器
# ★ 只执行"用户在网页上显式开启"的定时任务；默认没有任何定时。
SCHEDULER_STATUS = {"alive": False, "last_check": None, "note": "调度器已启动（仅在任务被开启定时后才会有动作）"}
WEEK_MAP = {0: "1", 1: "2", 2: "3", 3: "4", 4: "5", 5: "6", 6: "7"}   # 周一=1
# ★ 错过的任务最多补跑 60 分钟。超过就不再触发——否则晚上 22:00 打开网站，
#   会把早上 09:10 的盘前任务也补跑一次（盘前任务晚上跑毫无意义）。
GRACE_MIN = 60


def _scheduler_loop():
    SCHEDULER_STATUS["alive"] = True
    while True:
        try:
            SCHEDULER_STATUS["last_check"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            now = datetime.now()
            today = WEEK_MAP[now.weekday()]
            settings = db.get_task_settings()
            for key in tasks.TASK_KEYS:
                st = settings.get(key)
                if not st or not st.get("enabled"):
                    continue
                at = (st.get("at_time") or "").strip()
                days = (st.get("days") or "").replace(" ", "")
                if not at or today not in days.split(","):
                    continue
                try:
                    hh, mm = [int(x) for x in at.split(":")]
                except Exception:
                    continue
                due = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
                if now < due:
                    continue
                # ★ 宽限窗口：只补跑「刚错过不久」的，避免开机后补跑过期任务
                if (now - due).total_seconds() > GRACE_MIN * 60:
                    continue
                # 今天该时间点之后是否已经跑过（防重复触发；失败也算跑过，不会无限重试）
                if db.has_run_since(key, due.isoformat(timespec="seconds")):
                    continue
                # 同一时刻只允许一个任务在跑（本进程 + 跨进程都要检查）
                if any(tasks.STATE[k]["running"] for k in tasks.TASK_KEYS):
                    continue
                try:
                    if db.has_running_task():        # ★ 控制台进程可能正在跑
                        continue
                except Exception:
                    pass
                print(f"[调度器] 触发定时任务：{key}（设定 {at}）", flush=True)
                tasks.run_task_async(key, "schedule")
        except Exception:
            traceback.print_exc()
        _sleep(20)


def _sleep(sec: int):
    import time
    time.sleep(sec)


# ------------------------------------------------------------------ 入口
def main():
    db.init_db()
    host, port = "127.0.0.1", 8000
    print("=" * 68)
    print("  乖离率策略 · 本地工具站已启动")
    print(f"  请在浏览器打开：  http://{host}:{port}")
    print(f"  门槛：广度 ≥ {core.CFG['breadth_threshold']}　|　BIAS ≤ {core.CFG['bias_threshold']}%")
    print("  提示：三个任务默认都是「不定时」，需要在网页上手动开启。")
    print("  按 Ctrl+C 关闭")
    print("=" * 68, flush=True)

    threading.Thread(target=_scheduler_loop, daemon=True).start()

    try:
        app.run(host=host, port=port, debug=False, threaded=True)
    except OSError as e:
        print("\n" + "!" * 68)
        print(f"  启动失败：端口 {port} 已被占用（{e}）")
        print("  说明：工具站可能已经在运行了——请直接在浏览器打开")
        print(f"        http://{host}:{port}")
        print("  若想重启：先用「控制台」关闭，或关掉之前那个黑色窗口。")
        print("!" * 68)
        try:
            input("\n按回车键退出…")
        except Exception:
            pass


if __name__ == "__main__":
    main()
