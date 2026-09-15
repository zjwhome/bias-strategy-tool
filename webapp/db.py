# -*- coding: utf-8 -*-
"""
乖离率策略 · 数据库模块 (db.py)
本地 SQLite，零安装。存三类数据：
  1. daily       —— 每日广度快照
  2. candidates  —— 每日候选股
  3. holdings    —— 我的持仓 / 交易记录
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS daily (
    date          TEXT PRIMARY KEY,
    breadth       INTEGER,
    threshold     INTEGER,
    triggered     INTEGER,
    bias_only     INTEGER,
    stocks_total  INTEGER,
    updated_at    TEXT
);

CREATE TABLE IF NOT EXISTS candidates (
    date          TEXT,
    code          TEXT,
    name          TEXT,
    close         REAL,
    bias          REAL,
    turnover_pct  REAL,
    vol_ratio     REAL,
    chg5d         REAL,
    amount        REAL,
    rank          INTEGER,
    PRIMARY KEY (date, code)
);

CREATE TABLE IF NOT EXISTS holdings (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    code          TEXT NOT NULL,
    name          TEXT,
    buy_date      TEXT NOT NULL,
    buy_price     REAL NOT NULL,
    shares        INTEGER DEFAULT 0,
    stop_price    REAL,
    tp1_price     REAL,
    tp2_price     REAL,
    peak_price    REAL,
    status        TEXT DEFAULT 'holding',
    sell_date     TEXT,
    sell_price    REAL,
    sell_reason   TEXT,
    note          TEXT,
    created_at    TEXT
);

CREATE INDEX IF NOT EXISTS idx_cand_date ON candidates(date);
CREATE INDEX IF NOT EXISTS idx_hold_status ON holdings(status);

-- 任务定时设置（★ 默认全部关闭，由用户主动开启；网站绝不自己定时）
CREATE TABLE IF NOT EXISTS task_settings (
    key        TEXT PRIMARY KEY,      -- premarket / intraday / postmarket
    enabled    INTEGER DEFAULT 0,
    at_time    TEXT,                  -- "09:10"
    days       TEXT,                  -- "1,2,3,4,5" = 周一~周五
    updated_at TEXT
);

-- 任务执行历史
CREATE TABLE IF NOT EXISTS task_runs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    task_key    TEXT,
    trigger_by  TEXT,                 -- manual / schedule
    started_at  TEXT,
    finished_at TEXT,
    status      TEXT,                 -- running / ok / fail
    title       TEXT,
    summary     TEXT,                 -- JSON：结构化结果
    detail      TEXT                  -- 文本明细
);

CREATE INDEX IF NOT EXISTS idx_run_key ON task_runs(task_key, id);
"""


class _Conn(sqlite3.Connection):
    """让 `with connect() as conn:` 既能提交/回滚事务，又能在退出时**真正关闭**连接。

    ⚠️ 标准库的 `sqlite3.Connection` 上下文管理器只做 commit/rollback，**不会 close**。
    本工具是长期驻留的服务（Flask 每个请求、调度器每 20 秒轮询都会开连接），
    若不显式关闭，会不断堆积文件句柄，最终表现为「数据库被占用 / 打不开」。
    """

    def __exit__(self, exc_type, exc, tb):
        try:
            return super().__exit__(exc_type, exc, tb)   # commit 或 rollback
        finally:
            self.close()                                  # ★ 关键：真正释放连接


def connect() -> sqlite3.Connection:
    """打开数据库连接。

    timeout / busy_timeout = 30 秒：
      网页请求、后台调度器、控制台进程可能同时写库，默认 5 秒会直接抛
      `database is locked`，这里放宽到 30 秒，让并发写能够排队完成而不是报错。
    """
    conn = sqlite3.connect(DB_PATH, timeout=30, factory=_Conn)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA busy_timeout=30000")
    except Exception:
        pass
    return conn


def init_db() -> None:
    with connect() as conn:
        try:
            # WAL 模式：读写并发友好（网页读的同时后台任务可以写），且是持久化设置
            conn.execute("PRAGMA journal_mode=WAL")
        except Exception:
            pass
        conn.executescript(SCHEMA)


def save_daily(snap: dict) -> None:
    """写入/更新某日广度快照"""
    with connect() as conn:
        conn.execute(
            """INSERT INTO daily(date, breadth, threshold, triggered, bias_only, stocks_total, updated_at)
               VALUES(?,?,?,?,?,?,?)
               ON CONFLICT(date) DO UPDATE SET
                 breadth=excluded.breadth, threshold=excluded.threshold,
                 triggered=excluded.triggered, bias_only=excluded.bias_only,
                 stocks_total=excluded.stocks_total, updated_at=excluded.updated_at""",
            (snap["date"], snap["breadth"], snap["threshold"], int(snap["triggered"]),
             snap["bias_only"], snap["stocks_total"], datetime.now().isoformat(timespec="seconds")),
        )


def save_candidates(date: str, rows: list[dict]) -> None:
    """覆盖写入某日候选股"""
    with connect() as conn:
        conn.execute("DELETE FROM candidates WHERE date=?", (date,))
        for i, r in enumerate(rows, 1):
            conn.execute(
                """INSERT INTO candidates(date, code, name, close, bias, turnover_pct,
                                          vol_ratio, chg5d, amount, rank)
                   VALUES(?,?,?,?,?,?,?,?,?,?)""",
                (date, r.get("code"), r.get("name"), r.get("close"), r.get("bias"),
                 r.get("turnover_pct"), r.get("vol_ratio"), r.get("chg5d"), r.get("amount"), i),
            )


def get_daily(date: str | None = None) -> dict | None:
    with connect() as conn:
        if date:
            row = conn.execute("SELECT * FROM daily WHERE date=?", (date,)).fetchone()
        else:
            row = conn.execute("SELECT * FROM daily ORDER BY date DESC LIMIT 1").fetchone()
        return dict(row) if row else None


def get_candidates(date: str) -> list[dict]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM candidates WHERE date=? ORDER BY rank", (date,)).fetchall()
        return [dict(r) for r in rows]


def get_daily_history(days: int = 250) -> list[dict]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM daily ORDER BY date DESC LIMIT ?", (days,)).fetchall()
        return [dict(r) for r in reversed(rows)]


# ------------------------------------------------------------ 持仓
def add_holding(code: str, name: str, buy_date: str, buy_price: float,
                shares: int = 0, note: str = "") -> int:
    stop = round(buy_price * 0.94, 3)
    tp1 = round(buy_price * 1.06, 3)
    tp2 = round(buy_price * 1.10, 3)
    with connect() as conn:
        cur = conn.execute(
            """INSERT INTO holdings(code, name, buy_date, buy_price, shares,
                                    stop_price, tp1_price, tp2_price, peak_price,
                                    status, note, created_at)
               VALUES(?,?,?,?,?,?,?,?,?,'holding',?,?)""",
            (code, name, buy_date, buy_price, shares, stop, tp1, tp2, buy_price,
             note, datetime.now().isoformat(timespec="seconds")),
        )
        return cur.lastrowid


def list_holdings(status: str = "holding") -> list[dict]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM holdings WHERE status=? ORDER BY buy_date DESC", (status,)).fetchall()
        return [dict(r) for r in rows]


def close_holding(hid: int, sell_date: str, sell_price: float, reason: str = "") -> None:
    with connect() as conn:
        conn.execute(
            """UPDATE holdings SET status='closed', sell_date=?, sell_price=?, sell_reason=?
               WHERE id=?""",
            (sell_date, sell_price, reason, hid),
        )


def update_peak(hid: int, peak: float) -> None:
    with connect() as conn:
        conn.execute("UPDATE holdings SET peak_price=? WHERE id=?", (peak, hid))


def delete_holding(hid: int) -> None:
    with connect() as conn:
        conn.execute("DELETE FROM holdings WHERE id=?", (hid,))


# ------------------------------------------------------------ 任务（盘前/盘中/盘后）
def get_task_settings() -> dict:
    """返回 {key: {enabled, at_time, days}}"""
    with connect() as conn:
        rows = conn.execute("SELECT * FROM task_settings").fetchall()
    return {r["key"]: dict(r) for r in rows}


def set_task_setting(key: str, enabled: bool, at_time: str, days: str) -> None:
    with connect() as conn:
        conn.execute(
            """INSERT INTO task_settings(key, enabled, at_time, days, updated_at)
               VALUES(?,?,?,?,?)
               ON CONFLICT(key) DO UPDATE SET
                 enabled=excluded.enabled, at_time=excluded.at_time,
                 days=excluded.days, updated_at=excluded.updated_at""",
            (key, int(bool(enabled)), at_time, days,
             datetime.now().isoformat(timespec="seconds")),
        )


def start_task_run(task_key: str, trigger_by: str) -> int:
    with connect() as conn:
        cur = conn.execute(
            """INSERT INTO task_runs(task_key, trigger_by, started_at, status, title)
               VALUES(?,?,?, 'running', '执行中…')""",
            (task_key, trigger_by, datetime.now().isoformat(timespec="seconds")),
        )
        return cur.lastrowid


def finish_task_run(run_id: int, status: str, title: str,
                    summary: str = "", detail: str = "") -> None:
    with connect() as conn:
        conn.execute(
            """UPDATE task_runs SET finished_at=?, status=?, title=?, summary=?, detail=?
               WHERE id=?""",
            (datetime.now().isoformat(timespec="seconds"), status, title,
             summary, detail, run_id),
        )


def get_task_runs(task_key: str | None = None, limit: int = 20) -> list[dict]:
    with connect() as conn:
        if task_key:
            rows = conn.execute(
                "SELECT * FROM task_runs WHERE task_key=? ORDER BY id DESC LIMIT ?",
                (task_key, limit)).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM task_runs ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    return [dict(r) for r in rows]


def get_last_run(task_key: str) -> dict | None:
    rows = get_task_runs(task_key, limit=1)
    return rows[0] if rows else None


def has_run_since(task_key: str, since_iso: str) -> bool:
    """判断某任务在 since_iso 之后是否**已经尝试执行过**（用于防止同一时间点重复触发）。

    ★ 必须包含 'fail'：如果只算 'ok'，那么一次失败会让调度器每 20 秒重试一次，
      形成无限重试循环（日志被刷爆、数据库被反复写）。失败也算"尝试过"，
      当天不再自动重试；用户可以在网页上手动重跑。
    """
    with connect() as conn:
        n = conn.execute(
            """SELECT COUNT(*) FROM task_runs
               WHERE task_key=? AND started_at>=? AND status IN ('ok','fail','running')""",
            (task_key, since_iso)).fetchone()[0]
    return n > 0


def has_running_task(max_age_min: int = 120) -> list[str]:
    """返回**当前确实在运行**的任务 key 列表（跨进程可见）。

    控制台与网页是两个独立进程，各自的内存状态互不可见，因此用数据库
    `task_runs` 里 status='running' 的记录做跨进程互斥。
    超过 max_age_min 分钟仍未结束的记录视为**僵尸**（进程被强杀/断电），不计入。

    顺带把僵尸记录标记为 fail，避免它永久挡住后续调度。
    """
    cutoff = (datetime.now() - timedelta(minutes=max_age_min)).isoformat(timespec="seconds")
    with connect() as conn:
        rows = conn.execute(
            "SELECT task_key FROM task_runs WHERE status='running' AND started_at>=?",
            (cutoff,)).fetchall()
        # 清理僵尸：把过期的 running 标成 fail（否则会永久占位）
        conn.execute(
            """UPDATE task_runs SET status='fail', finished_at=?,
                   title='执行中断（进程异常退出）'
               WHERE status='running' AND started_at<?""",
            (datetime.now().isoformat(timespec="seconds"), cutoff))
    return [r["task_key"] for r in rows]


if __name__ == "__main__":
    init_db()
    print("数据库初始化完成 →", DB_PATH)
