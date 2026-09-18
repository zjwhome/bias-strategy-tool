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
  GET    /api/intraday              最近一次盘中扫描结果（「盘中参考」卡片）
  POST   /api/intraday/refresh      轻量重跑一次盘中扫描（不写任务记录，供实时更新）
  GET    /api/intraday/refresh      实时刷新的运行态
  GET    /api/holdings              我的持仓（?live=1 强制用实时行情重算）
  POST   /api/holdings              新增持仓
  POST   /api/holdings/<id>/close   平仓
  DELETE /api/holdings/<id>         删除持仓记录
  GET    /api/tasks                 三个任务的设置 + 运行态 + 最近执行
  POST   /api/tasks/<key>/run       立即执行任务
  POST   /api/tasks/<key>/schedule  设置/取消定时（{enabled, at_time, days}）
  GET    /api/tasks/<key>/runs      执行历史（含结果明细）
  GET    /api/tasks/<key>/result/<id>  某次执行的完整结果（分组块）
  POST   /api/update                直接跑一次全量数据更新（控制台用）
================================================================================
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time
import traceback
from datetime import date, datetime

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


def _int_arg(name: str, default: int, lo: int, hi: int) -> int:
    """读一个整数查询参数：非法值回落到默认值，合法值夹到 [lo, hi]。

    ★ 为什么必须有这道关：
      ① 以前是直接 `int(request.args.get("days", 250))`，浏览器地址栏里
         少打一个数字（?days=abc）就是 ValueError → HTTP 500，
         页面上表现为「加载失败」而完全不知道为什么。
      ② SQLite 的 `LIMIT -1` 意思是「**不限制**」而不是「0 条」——
         传 ?days=-1 会一次性把整张 daily 表（几千行）读出来，
         传 ?limit=-1 会把全部历史执行记录连同 7~9KB 的结果 JSON 一起返回。
         所以下界必须夹到 1。
      ③ 上界是防「?days=999999999」这类请求让 SQLite 白白铺全表。
    """
    raw = request.args.get(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        v = int(str(raw).strip())
    except (TypeError, ValueError):
        return default
    return max(lo, min(hi, v))


def _bool_val(v) -> bool:
    """把请求体里的开关值解析成布尔。

    ★ 不能直接 `bool(v)`：Python 里 **`bool("false")` 是 True**。
      前端传的是真正的布尔值（取自 input.checked），但控制台脚本 / curl
      常常传字符串 `"false"`，那样「关闭定时」会被执行成「开启定时」——
      而且界面上会显示成开启状态，用户完全看不出是被自己的传参坑了。
    """
    if isinstance(v, str):
        return v.strip().lower() in ("1", "true", "yes", "on", "y", "t")
    return bool(v)


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
        return jsonify(dict(ok=False, msg="尚无数据，请先到「盘后总结」点一次「更新数据」跑一次盘后任务。"))
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
    # 容错 + 夹区间，见 _int_arg 的说明（非法值回落 250，负数会被夹成 1）
    days = _int_arg("days", 250, 1, 5000)
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
        return jsonify(dict(ok=False, msg="还没有盘中扫描结果。到「盘中参考」点一次「⟳ 实时更新」即可。"))
    # ★ 不能写 dict(ok=..., **p)：p 里本来就有 ok 键，会抛
    #   "got multiple values for keyword argument 'ok'" → 接口 500。
    out = dict(got["payload"])
    out["created_at"] = got["created_at"]
    return jsonify(out)


# ======================================================= 盘中参考 · 实时刷新
"""
★ 为什么不直接复用「盘中任务」：

  任务执行会往 task_runs 写一条完整结果（实测 7~9 KB），并进入「历史执行记录」。
  而实时刷新是每 30~60 秒一次的动作，14:30~15:00 半小时就是 30 条 —— 一年下来
  几十 MB 的纯噪声，还会把真正的手动执行记录挤出历史列表（列表只留最近 20 条）。

  它语义上也确实不是一次「任务执行」，只是「按最新行情再看一眼」。
  所以走独立轻量通道：只跑扫描 + 覆盖 scan_cache，不落任务记录。

  另外必须与任务执行互斥：盘后任务正在重写全市场 CSV，此刻读 CSV 可能读到
  写了一半的文件；而且两边同时拉全市场快照也纯属浪费。
"""
_REFRESH = dict(running=False, msg="", n=0, last_at=None, last_ok=None,
                last_err="", dur=0.0)
_REFRESH_LOCK = threading.Lock()


def _refresh_busy() -> bool:
    """盘中「⟳ 实时更新」是否正在跑。

    ★ 为什么必须**双向**互斥：
      刷新侧（api_intraday_refresh）已经会挡「任务执行中」，
      但任务侧以前完全不检查刷新。于是用户在「盘中参考」点完实时更新、
      紧接着点「立即执行盘后任务」（或恰好撞上定时任务到点），
      盘后任务就会在刷新正在读全市场 CSV 的同时重写这些 CSV ——
      可能读到写了一半的文件，扫描结果会莫名少一批股票，
      而且日志上一点异常都看不出来。两边的注释都写着"必须互斥"，实际只做了半边。
    """
    with _REFRESH_LOCK:
        return bool(_REFRESH["running"])


def _refresh_worker(data_date: str) -> None:
    t0 = time.time()

    def lg(s):
        with _REFRESH_LOCK:
            # 去掉日志前缀，这行要直接显示给用户看
            _REFRESH["msg"] = str(s).replace("[盘中] ", "")

    ok, err = False, ""
    try:
        scan = core.intraday_scan(data_date=data_date, log=lg)
        if scan.get("ok"):
            db.save_scan("intraday", scan)          # 页面「盘中参考」读这一份
            ok = True
            # ★ 用户要的「刷新盘中数据时，持仓一起更新」：
            #   顺手把持仓的实时价算一遍并缓存。放在 running 归位**之前** ——
            #   这样页面看到的「刷新完成」＝ 清单和持仓都已经是新的。
            _warm_hold_cache(data_date)
        else:
            err = str(scan.get("msg") or "扫描未成功")
    except Exception as e:
        err = f"{type(e).__name__}: {e}"
    with _REFRESH_LOCK:
        _REFRESH.update(running=False, msg="", last_ok=ok, last_err=err,
                        last_at=datetime.now().isoformat(timespec="seconds"),
                        dur=round(time.time() - t0, 1))
        if ok:
            _REFRESH["n"] += 1


@app.post("/api/intraday/refresh")
def api_intraday_refresh():
    """按最新行情重跑一次盘中扫描（不写任务执行记录）。"""
    with _REFRESH_LOCK:
        if _REFRESH["running"]:
            return jsonify(dict(ok=False, busy=True, msg="正在刷新中…"))
        _REFRESH["running"] = True        # ★ 同一把锁内「检查 + 占位」，防并发重入
    try:
        busy = [k for k in tasks.TASK_KEYS if tasks.STATE[k]["running"]]
        # ★ 「更新数据」按钮走的是另一条通道：它先把 UPDATE_STATE["running"] 置位，
        #   再起线程去跑盘后任务 —— 中间那一瞬 tasks.STATE 还全是 False。
        #   不认这个标志，刷新恰好落进那一瞬就会和盘后任务撞上（它马上要重写 CSV）。
        if not busy and UPDATE_STATE.get("running"):
            busy = ["postmarket"]
        if not busy:
            # 跨进程：控制台窗口可能正在跑任务，它的状态在另一个进程里
            try:
                busy = db.has_running_task()
            except Exception:
                busy = []
        if busy:
            with _REFRESH_LOCK:
                _REFRESH["running"] = False
            names = {"premarket": "盘前任务", "intraday": "盘中任务",
                     "postmarket": "盘后任务"}
            return jsonify(dict(ok=False, busy=True,
                                msg=f"{'、'.join(names.get(k, k) for k in busy)}正在执行，等它跑完再刷新"))
        d = db.get_daily() or {}
        threading.Thread(target=_refresh_worker, args=(d.get("date", ""),),
                         daemon=True).start()
    except Exception:
        with _REFRESH_LOCK:
            _REFRESH["running"] = False    # 起线程失败要回滚，别把刷新永久锁死
        raise
    return jsonify(dict(ok=True, msg="已开始刷新"))


@app.get("/api/intraday/refresh")
def api_intraday_refresh_state():
    """实时刷新的运行态（供页面倒计时/进度轮询）。"""
    with _REFRESH_LOCK:
        return jsonify(dict(ok=True, **_REFRESH))


# ------------------------------------------------------------------ 持仓
"""
★ 持仓的「实时价」（2026-09-18 新增）

  以前 /api/holdings 一律用本地日线里的**最近收盘价**：用户盘中打开「我的持仓」，
  看到的还是昨天（或上一交易日）的价，盈亏自然也是旧的 —— 明明在盯盘，数字却不动。

  现在的取法（三句话）：

   ① 「盘中参考」的实时刷新每跑完一次，**顺带**把持仓的实时价也算一遍并缓存在这里。
      这正是用户要的「刷新盘中数据时，持仓也一起更新」。
   ② 「我的持仓」页自己的那个刷新按钮走 `?live=1`，当场强制拉一次实时行情。
   ③ 页面默认加载时：缓存还新鲜（≤ _HOLD_TTL 秒、且持仓集合没变）就用缓存里的实时价，
      否则退回**收盘价**并明说"这是 MM-DD 收盘价"——绝不一边显示旧价一边让人以为那是实时。

  ★ 缓存必须带指纹：刚登记 / 删除一只票时签名立刻变化 → 缓存自动作废，
    不会拿旧结果冒充新持仓（这是最容易骗过眼睛的一类错）。
  ★ 实时行情取不到时一律**降级 + 说明**，不假装成功。
"""
_HOLD_TTL = 120          # 实时持仓快照的新鲜期（秒）
_HOLD = dict(ts=0.0, sig="", rows=None, quote_date="", quote_time="",
             session="closed", asof="", src="")
_HOLD_LOCK = threading.Lock()


def _today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _hold_sig(rows: list[dict], ec: dict | None = None) -> str:
    """持仓指纹 = 持仓集合（id + 代码 + 买入价）**加上**出场参数。

    加/删/改一只票、或改了止损止盈 → 指纹立刻变 → 缓存自动作废。

    ★ 为什么出场参数必须进指纹：缓存里的 rows 是**按当时的止损/止盈算出来的**
      （action / alerts / 颜色都写死在结果里）。若用户把止损从 -6% 改成 -8%，
      持仓集合没变、缓存也没过期，页面就会出现「标题写着 -8%、表格里的动作却还是
      按 -6% 判的」—— 两处互相打架，最长持续 _HOLD_TTL 秒。带上参数，改完立刻重算。
    """
    base = "|".join(f"{r.get('id')}:{r.get('code')}:{r.get('buy_price')}"
                    for r in sorted(rows, key=lambda x: (x.get("id") or 0)))
    if not ec:
        return base
    tp = ec.get("take_profit_tiers_pct") or [0, 0]
    return (f"{base}#{ec.get('hard_stop_loss_pct')}:{tp[0]}:{tp[1]}:"
            f"{ec.get('trailing_trigger_pct')}:{ec.get('trailing_drawdown_pct')}:"
            f"{ec.get('max_hold_days')}")


def _quote_session(qdate: str, qtime: str) -> str:
    """实时快照属于「交易中」还是「已收盘」。

    ★ 15:00 之后实时价就是收盘价，再按秒去拉不会有新信息 ——
      前端据此自动收手（与「盘中参考」的实时更新同一套逻辑）。
    """
    if qdate != _today_str():
        return "closed"
    try:
        h, mi = (int(x) for x in str(qtime).split(":")[:2])
        return "trading" if core.time_progress(h, mi) < 1.0 else "closed"
    except Exception:
        return "closed"


def _clock_session() -> str:
    """按本机时钟判断「现在还需要不需要刷新行情」。

    ★ 为什么降级（收盘价）那条路上**不能**直接写 session="closed"：
      取不到实时行情时页面拿到的是收盘价，但那不代表"已经收盘"。
      若这里回 closed，前端的"收盘自动收手"会被触发，用户会看到
      「已收盘」这个错误的停止理由 —— 真相是网络/数据源出了问题。
      所以降级路径用**时钟**判断：交易时段内就让它继续重试，
      超时/收盘了才收手（前端另有连续失败 3 次自动停止兜底）。
    """
    now = datetime.now()
    if now.weekday() >= 5:                 # 周末不必刷
        return "closed"
    try:
        return "trading" if core.time_progress(now.hour, now.minute) < 1.0 else "closed"
    except Exception:
        return "closed"


def _eval_live(hs: list[dict]) -> dict:
    """给这一批持仓拉一次实时行情并体检。

    返回 dict(rows, got, total, quote_date, quote_time, src, err)；
    **rows 为 None 表示这次不能标「实时」**（调用方必须降级，不能当成功）。

    ★ 为什么必须「一只都不能少」才算成功（2026-09-18 审查后收紧）：
      `evaluate_holdings` 对**没有实时价**的那只会静默回落到本地日线的收盘价
      （tasks.py evaluate_holding 的 price=None 分支）。也就是说，2 只持仓里
      只要有 1 只没取到实时价，拼出来的表就是「一半今天的价 + 一半昨天的价」。
      这种表若整体标 `live=true`，界面上那格昨天的旧价会被当成实时价读 ——
      用户照着它算盈亏、甚至照着它决定卖不卖。所以口径定为：
      **要么整表实时，要么整表降级 + 明说「x/N 只有实时价」。**
      持仓一般 ≤10 只，all-or-nothing 的代价只是偶尔退回收盘价，值得。
    """
    codes = [str(h["code"]).zfill(6) for h in hs]
    try:
        raw = core.fetch_live_quotes(codes) or {}
    except Exception as e:
        return dict(rows=None, got=0, total=len(codes),
                    err=f"{type(e).__name__}: {e}")
    quotes, qdate, qtime, src = {}, "", "", ""
    for c in codes:
        q = raw.get(c)
        if q and q.get("close"):
            quotes[c] = float(q["close"])
            qdate = q.get("date") or qdate
            qtime = q.get("time") or qtime
            src = q.get("source") or src
    got, total = len(quotes), len(codes)
    if not quotes:
        return dict(rows=None, got=0, total=total, err="数据源没有返回任何价格")
    if got < total:
        miss = "、".join(c for c in codes if c not in quotes)
        return dict(rows=None, got=got, total=total, err=f"{miss} 没有实时价")
    try:
        rows = tasks.evaluate_holdings(quotes=quotes,
                                       quote_date=qdate or _today_str(),
                                       latest_market_date=_today_str(),
                                       intraday=True)
    except Exception as e:
        return dict(rows=None, got=0, total=total, err=f"{type(e).__name__}: {e}")
    return dict(rows=rows, got=got, total=total,
                quote_date=qdate, quote_time=qtime, src=src, err="")


def _warm_hold_cache(mdate: str = "") -> None:
    """把持仓的实时体检结果算一遍写进缓存（「盘中参考」实时刷新顺带调用）。

    刻意吞掉所有异常：持仓拉不到行情，不该把一次成功的全市场扫描判成失败。
    """
    t0 = time.time()
    try:
        hs = tasks.evaluate_holdings(latest_market_date=mdate or "")
        if not hs:
            with _HOLD_LOCK:
                _HOLD.update(ts=0.0, sig="", rows=None)
            return
        ec = tasks.exit_config()
        ev = _eval_live(hs)
        if ev["rows"] is None:
            # ★ 这一轮没拿到完整实时价，但缓存里可能还躺着**刚刚**拿到的实时价
            #   （≤ _HOLD_TTL 秒）。那种情况继续用它是对的 —— 它确实还是实时数据，
            #   清掉反而把好数据变成收盘价。新鲜度由 TTL 兜、身份由指纹兜，
            #   这里不动缓存，只留服务端痕迹便于排查。
            return
        with _HOLD_LOCK:
            if t0 < _HOLD["ts"]:      # 已经有更新的快照 → 拒旧盖新
                return
            _HOLD.update(ts=t0, sig=_hold_sig(hs, ec), rows=ev["rows"],
                         quote_date=ev["quote_date"], quote_time=ev["quote_time"],
                         session=_quote_session(ev["quote_date"], ev["quote_time"]),
                         asof=ev["quote_time"] or ev["quote_date"], src=ev["src"])
    except Exception:
        pass


def _task_busy() -> str:
    """现在有没有任务正在改写数据？有就返回一句人话，没有返回空串。

    ★ 为什么要把这件事告诉用户：盘后任务跑的时候，本地日线是**边写边变**的，
      这期间刷出来的持仓价可能一部分来自新数据、一部分来自旧数据（数字参差）。
      与其让用户盯着跳动的数字犯疑，不如在提示里直接说明白。
    """
    try:
        if _REFRESH.get("running"):
            return "「盘中参考」的实时刷新正在跑"
        for k, label in (("postmarket", "盘后任务"), ("intraday", "盘中任务"),
                         ("premarket", "盘前任务")):
            if (tasks.STATE.get(k) or {}).get("running"):
                return f"{label}正在跑"
    except Exception:
        pass
    return ""


def _hold_payload(force_live: bool) -> dict:
    """组装 /api/holdings 的返回体。force_live=True 表示必须当场拉实时行情。

    `session` 的口径统一成一句话：**"现在还有没有必要刷实时行情"** ——
    前端的「收盘自动收手」只看这一个字段。实时成功时由快照时间判定，
    降级/默认时由本机时钟判定（见 _clock_session 的注释）。
    """
    d = db.get_daily() or {}
    mdate = d.get("date", "")
    hs = tasks.evaluate_holdings(latest_market_date=mdate)
    ec = tasks.exit_config()
    # 出场参数一并返回给前端：max_hold_days 用于标记「已超期」，
    # exit 用于让卡片标题里的「止损 -6% · 止盈 +6%/+10%」跟着配置走，不再写死。
    base = dict(exit=dict(stop_pct=ec["hard_stop_loss_pct"],
                          tp1_pct=ec["take_profit_tiers_pct"][0],
                          tp2_pct=ec["take_profit_tiers_pct"][1],
                          max_hold_days=ec["max_hold_days"]),
                max_hold_days=ec["max_hold_days"],
                busy=_task_busy(), partial=False)
    # 本地可能一张日线都还没有（全新装的库）→ 不能拼出「下面显示的是  收盘价」
    day = f"{mdate} " if mdate else ""

    if not hs:
        # 空仓：不必浪费一次网络请求
        base.update(holdings=[], live=False, data_mode="close",
                    session=_clock_session(), asof=mdate, quote_date=mdate,
                    quote_time="", src="", note="当前没有持仓记录。")
        return base

    now = time.time()
    sig = _hold_sig(hs, ec)
    with _HOLD_LOCK:
        cache = dict(_HOLD)
    fresh = (cache["rows"] is not None and cache["sig"] == sig
             and now - cache["ts"] <= _HOLD_TTL)

    if not force_live and fresh:
        base.update(holdings=cache["rows"], live=True, data_mode="live",
                    session=cache["session"], asof=cache["asof"],
                    quote_date=cache["quote_date"], quote_time=cache["quote_time"],
                    src=cache["src"], note="")
        return base

    partial = False
    if force_live:
        t0 = time.time()                      # 记下起跑时刻，用于「拒旧盖新」
        ev = _eval_live(hs)
        if ev["rows"] is not None:
            ses = _quote_session(ev["quote_date"], ev["quote_time"])
            asof = ev["quote_time"] or ev["quote_date"]
            with _HOLD_LOCK:
                # ★ 两个 live 请求并发时，谁先起跑谁的数据更新 —— 后完成的**旧**请求
                #   不许把先完成的新快照盖回去（价差可能只有几秒，但方向必须是单调的）。
                if t0 >= _HOLD["ts"]:
                    _HOLD.update(ts=t0, sig=sig, rows=ev["rows"],
                                 quote_date=ev["quote_date"],
                                 quote_time=ev["quote_time"],
                                 session=ses, asof=asof, src=ev["src"])
            base.update(holdings=ev["rows"], live=True, data_mode="live", session=ses,
                        asof=asof, quote_date=ev["quote_date"],
                        quote_time=ev["quote_time"], src=ev["src"], note="")
            return base
        if ev["got"]:
            # 部分成功：**整表不许标实时**，缺价的那几只前端显示的是收盘价。
            # partial=True 让前端知道「这不是网络坏了，是某几只票没价」
            #   → 状态行亮降级原因，但**不算作连续失败**，别把自动刷新掐掉
            #   （停牌股几天都拿不到价，掐掉刷新等于让其余持仓也停止更新）。
            partial = True
            note = (f"实时价只拿到 {ev['got']}/{ev['total']} 只（{ev['err']}），"
                    f"缺价那几只显示的是{day}收盘价 —— 整表未标「实时」。")
        else:
            note = f"实时行情暂时取不到（{ev['err']}），下面显示的是{day}收盘价。"
    else:
        note = f"下面显示的是{day}收盘价。点「⟳ 刷新持仓」可以拿到实时价。"

    busy = base["busy"]
    if busy:
        note += f"\n★ {busy}，此刻数字可能新旧参差，等它跑完再看更准。"

    base.update(holdings=hs, live=False, data_mode="close", partial=partial,
                session=_clock_session(), asof=mdate, quote_date=mdate,
                quote_time="", src="", note=note)
    return base


@app.get("/api/holdings")
def api_holdings():
    force_live = str(request.args.get("live", "")).lower() in ("1", "true", "yes")
    return jsonify(dict(ok=True, **_hold_payload(force_live)))


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

    # ★ 校验并**归一化**买入日期。这里有两个坑：
    #   ① strptime 会宽松接受 '2026-9-16'（月/日不补零），它确实能过校验，
    #      但存到库里就是 '2026-9-16'；而持仓列表是 `ORDER BY buy_date DESC`
    #      按**字符串**排的 —— '2026-9-16' > '2026-10-01'（因为 '9' > '1'），
    #      排序会直接错位。所以先 strftime 成零补位的标准写法再入库。
    #   ② 允许未来日期（手滑打成 2027）会让「已持有 N 天」变成负数，
    #      止损/止盈的持有期判断全部失准 → 在入口挡掉。
    buy_date = str(j["buy_date"]).strip()
    try:
        bd = datetime.strptime(buy_date, "%Y-%m-%d").date()
    except ValueError:
        return jsonify(dict(ok=False, msg="买入日期格式应为 YYYY-MM-DD，例如 2026-09-16")), 400
    buy_date = bd.strftime("%Y-%m-%d")
    if bd > date.today():
        return (jsonify(dict(ok=False, msg=f"买入日期 {buy_date} 还没到"
                                          f"（今天是 {date.today():%Y-%m-%d}）")), 400)

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
    # ★ 先确认这条记录真的存在、且确实还在持仓中。
    #   以前是「先无脑执行、再无条件返回 ok:true」——
    #   对一个不存在的 id（页面开着没刷新、记录已在另一个标签页被删）也会回"成功"，
    #   用户以为平仓了，刷新后记录原样躺在那里；
    #   如果是重复提交，还会把已经写好的卖出日期/价格**静默改写**掉。
    #   这类"假成功"是最难排查的一类缺陷：界面全绿，数据没动。
    h = db.get_holding(hid)
    if not h:
        return jsonify(dict(ok=False, msg="找不到这条持仓记录（可能已被删除）")), 404
    if h.get("status") != "holding":
        return jsonify(dict(ok=False, msg="这条持仓已经是平仓状态了")), 409
    j = request.get_json(force=True, silent=True) or {}
    try:
        sp = float(j.get("sell_price") or 0)
    except (TypeError, ValueError):
        return jsonify(dict(ok=False, msg="卖出价格式不正确")), 400
    if sp <= 0:
        return jsonify(dict(ok=False, msg="卖出价必须大于 0")), 400
    sd = str(j.get("sell_date") or datetime.now().strftime("%Y-%m-%d")).strip()
    try:
        sdd = datetime.strptime(sd, "%Y-%m-%d").date()
    except ValueError:
        return jsonify(dict(ok=False, msg="卖出日期格式应为 YYYY-MM-DD")), 400
    sd = sdd.strftime("%Y-%m-%d")       # 归一化，见新增持仓处的说明
    if sdd > date.today():
        return jsonify(dict(ok=False, msg=f"卖出日期 {sd} 还没到")), 400
    # ★ 卖出不可能早于买入：早于买入会算出负的持有天数，出场复盘全乱。
    #   这里按「解析后的日期」比，不按字符串比 —— 老记录可能是 '2026-9-16'
    #   这种非零补位写法，字符串比较会给出错误结论。
    try:
        bdd = datetime.strptime(str(h.get("buy_date") or ""), "%Y-%m-%d").date()
    except ValueError:
        bdd = None
    if bdd and sdd < bdd:
        return (jsonify(dict(ok=False,
                             msg=f"卖出日期 {sd} 早于买入日期 {bdd:%Y-%m-%d}，请检查")), 400)
    if not db.close_holding(hid, sd, sp, j.get("reason", "手动平仓")):
        # 正常到不了这里（上面已校验过状态），但并发下仍可能被抢先平掉
        return jsonify(dict(ok=False, msg="这条持仓刚刚已被平仓，请刷新页面")), 409
    return jsonify(dict(ok=True))


@app.delete("/api/holdings/<int:hid>")
def api_delete_holding(hid):
    # ★ 不存在时回 404，而不是 ok:true。删除是最需要"确实删掉了"的动作，
    #   回一个假成功会让用户反复点击、却找不到问题出在哪。
    if not db.delete_holding(hid):
        return jsonify(dict(ok=False, msg="找不到这条持仓记录（可能已被删除）")), 404
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
    # ★ 反向闸门：见 _refresh_busy 的说明。实时更新只跑几秒，
    #   等它一下比让盘后任务读到写了一半的 CSV 划算得多。
    if _refresh_busy():
        return jsonify(dict(ok=False,
                            msg="「盘中参考」的实时更新正在跑，等它结束（通常几秒）再执行。"))
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
    enabled = _bool_val(j.get("enabled"))
    # ★ 一律先 str() 再 strip()：body 里把 at_time 写成数字（{"at_time":1535}）
    #   或 days 写成数组时，直接 .strip() 会抛 AttributeError → 500。
    at_time = str(j.get("at_time") or tasks.TASK_DEFS[key]["suggest"]).strip()
    raw_days = str(j.get("days") if j.get("days") is not None else "").strip()
    # 校验时间格式
    try:
        hh, mm = [int(x) for x in at_time.split(":")]
        assert 0 <= hh <= 23 and 0 <= mm <= 59
    except Exception:
        return jsonify(dict(ok=False, msg="时间格式应为 HH:MM，例如 17:30")), 400
    # ★ 校验星期：只接受 1~7（1=周一），去重后按数字排序。
    #   以前这里完全不校验，{"days":"9,x"} 会被原样存库 —— 而调度器是按
    #   `today in days.split(",")` 匹配的，非法值永远匹配不上：
    #   定时**静默地永不触发**，界面上却还老老实实显示「每天 17:30」。
    #   顺手把中文逗号也认掉（用户从文档里复制的多是全角）。
    parts = [p.strip() for p in raw_days.replace("，", ",").split(",") if p.strip()]
    bad = [p for p in parts if p not in ("1", "2", "3", "4", "5", "6", "7")]
    if bad:
        return (jsonify(dict(ok=False, msg="星期只能填 1~7 的数字（1=周一），"
                                           f"收到：{'、'.join(bad[:5])}")), 400)
    days = ",".join(sorted(set(parts), key=int))
    if not days:
        if enabled:
            # ★ 开启定时却一天都没勾 = 一条永远不会触发的规则。
            #   前端已经拦了，但**接口才是真正的关口**（控制台/脚本也打这里）。
            return jsonify(dict(ok=False, msg="开启定时时，请至少勾选一个星期。")), 400
        # 关闭定时时没勾任何一天 → 保留库里原来的星期（顺手滤掉历史脏值），
        # 免得"关一次就把星期清空"，下次想开启还得把七天重新勾一遍。
        prev = str(((db.get_task_settings().get(key) or {}).get("days")) or "")
        days = ",".join(d for d in prev.replace(" ", "").split(",")
                        if d in ("1", "2", "3", "4", "5", "6", "7")) or "1,2,3,4,5"
    db.set_task_setting(key, enabled, f"{hh:02d}:{mm:02d}", days)
    return jsonify(dict(ok=True, enabled=enabled, at_time=f"{hh:02d}:{mm:02d}", days=days,
                        msg=("定时已开启" if enabled else "定时已关闭")))


@app.get("/api/tasks/<key>/runs")
def api_task_runs(key):
    # ★ 与 /run、/schedule 对齐：未知任务键直接 404，而不是"空列表 + ok:true"。
    #   静默返回空集会让调用方以为「这个任务没跑过」，而不是「键写错了」。
    if key not in tasks.TASK_KEYS:
        return jsonify(dict(ok=False, msg="未知任务")), 404
    # 容错 + 夹区间：每条记录含 7~9KB 结果 JSON，上界 200 与之匹配
    limit = _int_arg("limit", 20, 1, 200)
    rows = db.get_task_runs(key, limit=limit)
    rows.reverse()                       # 旧 → 新，方便前端展示
    return jsonify(dict(ok=True, runs=rows))


@app.get("/api/tasks/<key>/result/<int:run_id>")
def api_task_result(key, run_id):
    if key not in tasks.TASK_KEYS:
        return jsonify(dict(ok=False, msg="未知任务")), 404
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
    # ★ 反向闸门：实时更新正在读全市场 CSV 时，不能同时去重写它（见 _refresh_busy）。
    #   放在取 _UPDATE_LOCK **之前**：避免"持着 _UPDATE_LOCK 再取 _REFRESH_LOCK"，
    #   与刷新线程的加锁顺序保持一致，杜绝死锁的可能。
    if _refresh_busy():
        return jsonify(dict(ok=False,
                            msg="「盘中参考」的实时更新正在跑，等它结束（通常几秒）再更新。"))
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
                # ★ 盘中「实时更新」正在读全市场 CSV 时也不能开跑（见 _refresh_busy）。
                #   跳过是安全的：本循环 20 秒一轮，而刷新只跑几秒；
                #   万一真的拖久了，GRACE_MIN 的 60 分钟宽限窗口还兜得住。
                if _refresh_busy():
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
        # ★ 这里刻意不用 app.run()，而是自己建服务器 —— 为了把监听套接字的
        #   socket 超时显式清掉。
        #
        #   背景：strategy_core 在模块顶层做了 socket.setdefaulttimeout(20)，
        #   那是为了治 akshare 的挂死请求（它内部 requests.get() 一个 timeout
        #   都不传，实测 5017 只里有 8 只会永久挂住，导致每日更新卡死）。
        #
        #   但 socket 的「默认超时」是**进程级**的，而本进程会 import 那个模块。
        #   于是 Werkzeug 的监听套接字、以及它 accept 出来的**每一个 HTTP 连接**，
        #   都被动带上了 20 秒超时。眼下所有接口都是毫秒级返回，看不出问题 ——
        #   可这是"靠运气活着"：将来任何一个慢一点的接口、或一次卡住的响应，
        #   都会在第 20 秒被凭空掐断，日志上只留一句莫名其妙的中断，
        #   排查起来极难（因为没人会想到超时是"隔壁模块顺手设的"）。
        #
        #   把下载用的 20 秒严格关在下载线程里：监听套接字 settimeout(None)
        #   之后，accept 出来的连接也是阻塞式的（CPython 的 socket.accept 只在
        #   "全局默认超时为 None" 时才强转阻塞，这里监听套接字自身超时为 None，
        #   已足够）。
        from werkzeug.serving import make_server
        srv = make_server(host, port, app, threaded=True)
        srv.socket.settimeout(None)   # 监听套接字：永不超时
        srv.timeout = None            # 内部 select 轮询：不设上限
        srv.serve_forever()
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
