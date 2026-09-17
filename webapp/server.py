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
import time
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


# ================================================================== 数据新鲜度探测
# ★ 要解决的问题：
#   本工具的全市场日线来自**新浪**（akshare 的 stock_zh_a_daily）。实测发现，
#   当天收盘后新浪并不会立刻发布当日 K 线 —— 2026-09-16 16:20 查，新浪的两个
#   独立接口（json_v2 日K、hisdata）最后一行都还停在 2026-09-15，而同一天腾讯
#   的日线接口已经有 09-16 了。
#   于是「跑完盘后任务，页面上还是昨天的数据」——用户会以为工具坏了。
#
#   这里用一个**独立数据源**（腾讯的上证指数日线）来判断「数据源目前最新可得
#   的交易日」，前端就能明确告诉用户：「不是你没跑，是数据源还没出数」。
#
#   设计原则：拿不到就返回 None（宁可不提示，也绝不误报）。
#   _EXPECT["next"] 是「下次允许出网的时间」，不是「上次出网的时间」——
#   成功时按 TTL 缓存 10 分钟；失败时只退避 30 秒。
#   ★ 这两档必须分开：服务刚起来的那一次探测必然拿不到值（异步预热），如果失败也
#     要等满 10 分钟，只要首次恰好赶上网络抖动，stale 自检就会瞎掉整整十分钟——
#     而它恰恰是「傍晚数据没出」时唯一的提醒。
_EXPECT = {"next": 0.0, "date": None, "fetching": False}
_EXPECT_TTL = 600                     # 成功：10 分钟缓存，避免每次轮询都出网
_EXPECT_TTL_FAIL = 30                 # 失败：30 秒后再试
# 取 3 根：因为盘中要把「今天那根还没走完的 K 线」剔掉，只用最后一根会没有回退值
_EXPECT_URL = ("https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
               "?param=sh000001,day,,,3,qfq")
# 收盘后多久才算「今天的日线已走完」。留 5 分钟给交易所与数据源落库。
_CLOSE_MIN = 15 * 60 + 5


def _fetch_expected_date():
    """查腾讯上证指数日线的最后一根 —— 即数据源当前最新**可得**的交易日。

    ★★ 必须在交易时段剔掉「今天」那一根。
       腾讯这个接口在盘中就会带上今天那根还没走完的 K 线，于是「最新可得交易日」
       会等于今天，而我们的库上一次更新还停在昨天 —— /api/status 就会报
       stale=True，页面从 09:30 到 15:00 一直挂着「⏳ 数据源还没发布当日行情」。
       这是**每个交易日的常态**（当天当然还没收盘），
       用户很快就会把这条提示当背景噪音忽略掉，
       等傍晚数据真的没出、最需要它提醒的时候反而看不见了。
       所以：今天那根在 15:05 之前一律不算「可得」。
    """
    import requests
    r = requests.get(_EXPECT_URL, timeout=6,
                     headers={"User-Agent": "Mozilla/5.0"})
    node = (r.json().get("data") or {}).get("sh000001") or {}
    rows = None
    for k in ("qfqday", "day"):
        if node.get(k):
            rows = node[k]
            break
    if not rows:
        return None
    out = [str(x[0])[:10] for x in rows if x and str(x[0])[:10]]
    if not out:
        return None
    now = datetime.now()
    today = now.strftime("%Y-%m-%d")
    if out[-1] == today and (now.hour * 60 + now.minute) < _CLOSE_MIN:
        out = out[:-1]                     # 今天这根还没走完 → 不算
    return out[-1] if out else None


def expected_trade_date():
    """返回「数据源目前最新可得的交易日」，探测失败或首次未就绪时返回 None。

    ★ 后台线程预热：绝不让网络请求卡住 /api/status（它每 8 秒被前端轮询一次）。
      第一次调用返回 None、8 秒后的下一次轮询就能拿到结果；
      探测失败也只退避 30 秒就重试，不会因为一次网络抖动瞎掉十分钟。
    """
    if time.time() < _EXPECT["next"] or _EXPECT["fetching"]:
        return _EXPECT["date"]
    _EXPECT["next"] = time.time() + 15   # 先占位，避免轮询时并发重复出网
    _EXPECT["fetching"] = True

    def _warm():
        got = None
        try:
            got = _fetch_expected_date()
            if got:
                _EXPECT["date"] = got
        except Exception:
            pass                          # 网络失败：保留上一次的值（可能为 None）
        finally:
            _EXPECT["next"] = time.time() + (_EXPECT_TTL if got else _EXPECT_TTL_FAIL)
            _EXPECT["fetching"] = False

    threading.Thread(target=_warm, daemon=True).start()
    return _EXPECT["date"]


def staleness(last_date):
    """对比「库里已有的最新交易日」和「数据源最新可得的交易日」。

    返回 (stale, expected, hint)。last_date 为空或探测不到时一律不告警。
    """
    exp = expected_trade_date()
    if not exp or not last_date or str(last_date) >= exp:
        return False, exp, ""
    hint = (f"数据源（新浪财经）目前只发布到 <b>{last_date}</b>，还没有 "
            f"<b>{exp}</b> 的行情，所以本次更新用的是 {last_date} 的数据。"
            f"<br>新浪的当日日线一般要等到<b>当天傍晚</b>才出 —— 稍晚一点"
            f"再跑一次「盘后任务」，数据就会前进到 {exp}。"
            f"<br>（交易时段不会提示这条：当天还没收盘，本来就不该有当日数据。）")
    return True, exp, hint


# ------------------------------------------------------------------ 静态页面
@app.route("/")
def index():
    return send_from_directory(WEBUI, "index.html")


@app.route("/<path:fname>")
def static_files(fname):
    return send_from_directory(WEBUI, fname)


@app.route("/favicon.ico")
def favicon():
    """浏览器默认会来要 /favicon.ico。以前落到通配路由上找不到文件 → 每条日志都
    多一行 404。这里直接把控制台图标（app.ico）给它，日志干净、标签页也有图标。"""
    return send_from_directory(HERE, "app.ico", mimetype="image/vnd.microsoft.icon")


# ------------------------------------------------------------------ 状态
@app.get("/api/status")
def api_status():
    core.reload_config()          # ★ 配置热更新：改了门槛不必重启服务（内部有 3 秒节流）
    d = db.get_daily()
    last_date = d["date"] if d else None
    running = [k for k in tasks.TASK_KEYS if tasks.STATE[k]["running"]]
    stale, exp, hint = staleness(last_date)
    data_version = f"{last_date}|{d['updated_at']}" if d else "empty"
    # ★ 版本号 = 数据版本 + 最近一次任务执行。
    #   靠数据版本只能发现「盘后任务」，盘中任务/盘前任务不写 daily 表，
    #   结果就是「任务跑了但页面不刷新」。把任务执行也并进版本号，任何任务跑完都会刷新。
    try:
        rid, rfin, rkey, rstat = db.get_last_run_id()
    except Exception:
        rid, rfin, rkey, rstat = 0, "", "", ""
    version = f"{data_version}#{rid}#{rfin}#{rstat}"
    return jsonify(dict(
        ok=True,
        last_date=last_date,
        updated_at=d["updated_at"] if d else None,
        breadth=d["breadth"] if d else None,
        breadth_main=(d["breadth_main"] if d else None),
        threshold=core.CFG["breadth_threshold"],
        threshold_main=core.CFG.get("breadth_threshold_main"),
        main_board_only=bool(core.CFG.get("main_board_only", True)),
        updating=UPDATE_STATE["running"],
        update_msg=UPDATE_STATE["msg"],
        running_tasks=running,
        # ★ 数据版本号：前端靠它判断"数据变了没有"，变了就整页刷新所有卡片。
        #   只要 save_daily 写过一次，updated_at 就会变（见 db.save_daily）。
        data_version=data_version,
        version=version,
        last_run=dict(id=rid, task=rkey, status=rstat, finished_at=rfin),
        expected_date=exp,
        stale=stale,
        stale_hint=hint,
        server_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    ))


@app.get("/api/today")
def api_today():
    core.reload_config()
    d = db.get_daily()
    if not d:
        return jsonify(dict(ok=False, msg="尚无数据，请先到「任务中心」跑一次「盘后任务」。"))
    # ★ 读取时按主板过滤：库里 2026-09-16 之前写入的候选股是「只看主板」规则
    #   生效前产生的，混着创业板（宁德时代/光韵达/阳光电源）。不回填历史数据，
    #   而是在读取层拦掉，用户看到的就始终是「只看沪深主板」的名单。
    cands = core.filter_main_board_rows(db.get_candidates(d["date"]))
    stale, exp, hint = staleness(d["date"])
    tm = core.CFG.get("breadth_threshold_main")
    return jsonify(dict(
        ok=True,
        date=d["date"],
        breadth=d["breadth"],
        bias_only=d["bias_only"],
        breadth_main=d.get("breadth_main"),
        bias_only_main=d.get("bias_only_main"),
        stocks_total=d["stocks_total"],
        threshold=d["threshold"],
        threshold_main=tm,
        main_board_only=bool(core.CFG.get("main_board_only", True)),
        triggered=bool(d["triggered"]),
        updated_at=d["updated_at"],
        expected_date=exp,
        stale=stale,
        stale_hint=hint,
        action=("✅ 达标：可以按下方候选股出手" if d["triggered"]
                else f"⛔ 未达标：今天不动手（需广度 ≥ {d['threshold']}）"),
        candidates=cands,
    ))


@app.get("/api/history")
def api_history():
    days = int(request.args.get("days", 250))
    rows = db.get_daily_history(days)
    return jsonify(dict(ok=True, threshold=core.CFG["breadth_threshold"],
                        threshold_main=core.CFG.get("breadth_threshold_main"),
                        rows=[dict(date=r["date"], breadth=r["breadth"],
                                   breadth_main=r.get("breadth_main"))
                              for r in rows]))


@app.get("/api/intraday")
def api_intraday():
    """最近一次「盘中任务」的扫描结果（供页面「盘中参考」卡片使用）。"""
    got = db.get_scan("intraday")
    if not got:
        return jsonify(dict(ok=False, msg="还没有盘中扫描结果。到「任务中心」跑一次「盘中任务」即可。"))
    # ★ 不能写 dict(ok=..., **p)：p 里本来就有 ok 键，会抛
    #   "got multiple values for keyword argument 'ok'" → 接口 500。
    out = dict(got["payload"])
    out["created_at"] = got["created_at"]
    return jsonify(out)


# ------------------------------------------------------------------ 持仓
@app.get("/api/holdings")
def api_holdings():
    d = db.get_daily()
    hs = tasks.evaluate_holdings(latest_market_date=d["date"] if d else "")
    # 出场参数一并返回给前端：max_hold_days 用于标记「已超期」，
    # exit 用于让卡片标题里的「止损 -6% · 止盈 +6%/+10%」跟着配置走，不再写死。
    ec = tasks.exit_config()
    return jsonify(dict(ok=True, holdings=hs,
                        max_hold_days=ec["max_hold_days"],
                        exit=dict(stop_pct=ec["hard_stop_loss_pct"],
                                  tp1_pct=ec["take_profit_tiers_pct"][0],
                                  tp2_pct=ec["take_profit_tiers_pct"][1],
                                  max_hold_days=ec["max_hold_days"])))


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
_UPDATE_LOCK = threading.Lock()


@app.post("/api/update")
def api_update():
    """等价于执行一次「盘后任务」（用户手动触发）"""
    # ★ running 必须在**同步路径**里置位，不能放到子线程里。
    #   曾经写成在线程内 `UPDATE_STATE.update(running=True)`，
    #   于是本函数返回、而线程还没被调度的那一瞬间，/api/status 仍回报
    #   updating=False —— 前端的轮询一旦落在这一瞬，就会立刻判定「已完成」、
    #   停掉轮询并去 loadAll()（拿到的是旧数据），此后这个页面再也不会自动刷新。
    with _UPDATE_LOCK:
        if UPDATE_STATE["running"] or any(tasks.STATE[k]["running"] for k in tasks.TASK_KEYS):
            return jsonify(dict(ok=False, msg="已有任务在运行中"))
        UPDATE_STATE.update(running=True, msg="正在更新数据…")

    def work():
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

    try:
        threading.Thread(target=work, daemon=True).start()
    except Exception as e:                      # 线程都起不来 → 必须把 running 放回去
        UPDATE_STATE.update(running=False, msg=f"更新失败：{e}")
        return jsonify(dict(ok=False, msg="无法启动更新线程")), 500
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
    _skip_notice = {"date": ""}
    while True:
        try:
            SCHEDULER_STATUS["last_check"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            now = datetime.now()
            today = WEEK_MAP[now.weekday()]
            ds = now.strftime("%Y-%m-%d")
            settings = db.get_task_settings()

            # ★ 节假日保护：**工作日 ≠ 交易日**。法定节假日（春节/国庆…）里这个循环
            #   照样会按「星期几」判定到期，然后触发一次盘后任务——白下载 20~25 分钟
            #   全市场数据。先用交易日历挡掉，只在「今天确实有开启的定时任务」时才查，
            #   避免用户从没开过定时也白白出网。is_trade_day() 返回 None（日历拉不到）
            #   时**放行**：宁可多跑一次，也绝不能因为日历缺失而漏跑。
            need_cal = False
            for k in tasks.TASK_KEYS:
                stk = settings.get(k) or {}
                if stk.get("enabled") and \
                        today in (stk.get("days") or "").replace(" ", "").split(","):
                    need_cal = True
                    break
            on_trade_day = True
            if need_cal:
                try:
                    on_trade_day = core.is_trade_day(ds) is not False
                except Exception:
                    on_trade_day = True
                if not on_trade_day and _skip_notice["date"] != ds:
                    _skip_notice["date"] = ds
                    print(f"[调度器] {ds} 是非交易日（节假日），今天的定时任务全部跳过",
                          flush=True)
                elif on_trade_day:
                    _skip_notice["date"] = ""

            for key in tasks.TASK_KEYS:
                st = settings.get(key)
                if not st or not st.get("enabled"):
                    continue
                if not on_trade_day:            # 节假日：不触发
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
    # ★ 端口可用环境变量 BIA_PORT 覆盖（默认 8000）。端口被占用时改这个即可，
    #   control.py 读的是同一个变量，两边不会脱节。
    host = os.environ.get("BIA_HOST", "127.0.0.1")
    try:
        port = int(os.environ.get("BIA_PORT") or 8000)
    except ValueError:
        port = 8000
    print("=" * 68)
    print("  乖离率策略 · 本地工具站已启动")
    print(f"  请在浏览器打开：  http://{host}:{port}")
    print(f"  门槛：广度 ≥ {core.CFG['breadth_threshold']}　|　BIAS ≤ {core.CFG['bias_threshold']}%")
    print(f"  数据库：{db.DB_PATH}")
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
