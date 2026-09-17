# -*- coding: utf-8 -*-
"""
================================================================================
乖离率策略 · 服务重启工具 (restart_server.py)
================================================================================
把工具网站**干净地重启成单实例**。日常用「控制台.bat」就够了；这个脚本解决的是
控制台解决不了的两个问题：

  ① **多实例抢端口**：Windows 的 SO_REUSEADDR 允许同一个端口被重复绑定，
     于是两个 server.py 可以同时「监听」:8000，请求被随机分发给新旧两个版本的
     代码 —— 表现是「一会儿有这个字段、一会儿没有」，极难排查。
     本脚本先杀掉所有在 Listen :8000 的进程，再只拉一个起来。

  ② **脱离会话**：直接 subprocess.Popen 起来的子进程会挂在当前终端/**作业对象**
     上，终端一关就被回收。这里借计划任务（schtasks）当父进程：包装脚本 Popen 出
     server.py 后立刻退出，server.py 随即成为孤儿进程，谁关都不影响它。
     ⚠️ 实测证据（2026-09-17）：直接 Popen(creationflags=DETACHED_PROCESS) 出来的
     服务，在**同一条** shell 命令里 `sleep 6` 还能 netstat 到监听，但下一条命令就
     已经没了 —— 说明它确实是被调用方的作业对象回收的，DETACHED 不够，必须绕计划任务。

用法：
    python restart_server.py            # 重启（有任务在跑会拒绝执行）
    python restart_server.py --force     # 无视运行中的任务，强杀重启

设计取舍（都是踩过的坑）：
  * **启动要能重试**。计划任务返回码全 0、包装脚本也执行了，server.py 仍可能没绑上
    端口（2026-09-17 09:41 实际发生过一次）。所以最多试 3 轮，每一轮都以
    「端口真的在监听 + /api/status 自报 ok」为准，绝不凭返回码宣布成功。
  * **删掉计划任务不会带停服务**（2026-09-17 实测）。任务是一次性的，用完即删。
  * **只用 netstat，不用 PowerShell**。netstat -ano 与 control.py 用的是同一条路径，
    任何终端里都能跑；而且「单实例」的正确判据本来就是
    **有几个进程在 Listen :8000**，不是 python 进程总数 ——
    venv 的 Scripts\\python.exe 只是个 launcher，会转调真实解释器并等待，
    天然形成「launcher(1 线程/4MB) → 真实 python(25 线程/90MB)」的父子对，属正常。
  * **不要顺手去杀 control.py**。它是控制台菜单，不占端口；而它可能正在跑任务，
    误杀会打断一次全量下载。脚本只碰真正监听端口的进程。
================================================================================
"""
from __future__ import annotations

import io
import json
import os
import subprocess
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable
TMP = os.environ.get("TEMP") or HERE
WRAP = os.path.join(TMP, "bia_restart_wrapper.py")
TASK = "BIA_RelayStart"
HOST = os.environ.get("BIA_HOST", "127.0.0.1")
try:
    PORT = int(os.environ.get("BIA_PORT") or 8000)
except ValueError:
    PORT = 8000

out = io.StringIO()


def p(*a):
    print(*a, file=out, flush=True)


# ---------------------------------------------------------------- 外部命令
def _run(cmd: list[str], timeout: int = 25):
    """执行外部命令 → (code, stdout, stderr)。

    ⚠️ Windows 命令行工具（netstat/taskkill）输出是 GBK/OEM 编码，
       直接 text=True 用 utf-8 解码会在读取线程里抛 UnicodeDecodeError，
       所以按字节捕获再做多编码尝试。
    """
    try:
        r = subprocess.run(cmd, capture_output=True, timeout=timeout)
    except Exception as e:
        return -1, "", f"{type(e).__name__}: {e}"

    def dec(b: bytes) -> str:
        for enc in ("utf-8", "gbk", "latin-1"):
            try:
                return b.decode(enc)
            except Exception:
                continue
        return (b or b"").decode("utf-8", "replace")

    return r.returncode, dec(r.stdout or b""), dec(r.stderr or b"")


def listeners() -> dict:
    """{pid: 本地地址} —— 正在 Listen 本端口的进程。"""
    res = {}
    code, txt, err = _run(["netstat", "-ano", "-p", "TCP"], timeout=25)
    if code != 0:
        p(f"  ⚠ netstat 执行失败（{code}）：{err.strip()[:120]}")
        return res
    for ln in txt.splitlines():
        f = ln.split()
        if len(f) >= 5 and f[3].upper() == "LISTENING" and f[1].endswith(f":{PORT}"):
            try:
                res[int(f[4])] = f[1]
            except Exception:
                continue
    return res


def status(timeout: int = 20) -> dict:
    try:
        with urllib.request.urlopen(f"http://{HOST}:{PORT}/api/status",
                                    timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        return dict(ok=False, err=f"{type(e).__name__}: {e}")


def kill(pid: int) -> int:
    return subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"],
                          capture_output=True).returncode


# ---------------------------------------------------------------- 脱离会话启动
WRAPPER_SRC = '''# -*- coding: utf-8 -*-
"""由计划任务执行的包装脚本：拉起 server.py 后立刻退出，使 server.py 成为孤儿进程。
本文件由 restart_server.py 自动生成。"""
import os, subprocess, time
WEBAPP = r"%s"
PY = r"%s"
NOTE = %s
LOG = os.path.join(WEBAPP, "server_console.log")
env = dict(os.environ)
# 环境变量原样继承：restart_server.py 是按 BIA_PORT/BIA_HOST/BIA_DB_PATH 检查的，
# 服务端必须用同一套，否则会出现「检查 8000、服务却起在 8100」的错位。
env["PYTHONUTF8"] = "1"
env["PYTHONIOENCODING"] = "utf-8"
log = open(LOG, "a", encoding="utf-8")
log.write("\\n===== [%%s] 通过计划任务启动工具网站%%s =====\\n" %% (
    time.strftime("%%Y-%%m-%%d %%H:%%M:%%S"), NOTE))
log.flush()
subprocess.Popen([PY, "server.py"], cwd=WEBAPP, stdout=log, stderr=log, env=env,
                 creationflags=0x00000008 | 0x00000200)   # DETACHED_PROCESS | NEW_PROCESS_GROUP
'''


def launch_detached(note: str = "") -> bool:
    """经计划任务把 server.py 拉起来（脱离本会话）。

    ★ 为什么必须绕计划任务：直接 subprocess.Popen 出来的进程仍挂在调用者的
      **作业对象**里，Bash/终端一回收，server.py 就跟着被杀 —— 实测过：
      同一次命令里能 netstat 到监听，下一条命令就没了。
      计划任务的父进程是「任务计划程序服务」，与调用者的会话无关，
      server.py 被 Popen 出来后包装脚本立刻退出 → server.py 成为孤儿进程，稳定存活。
    """
    try:
        with open(WRAP, "w", encoding="utf-8") as f:
            f.write(WRAPPER_SRC % (HERE, PY, repr(note)))
    except Exception as e:
        p(f"  ✘ 写包装脚本失败：{e}")
        return False
    ok = True
    for args in (["/create", "/tn", TASK, "/tr", f'"{PY}" "{WRAP}"',
                  "/sc", "once", "/st", "00:00", "/f"],
                 ["/run", "/tn", TASK]):
        code, _, err = _run(["schtasks"] + args, timeout=40)
        p(f"  schtasks {args[0]} → {code}" + (f"  {err.strip()[:100]}" if code else ""))
        ok = ok and code == 0
    time.sleep(1)
    # 删除任务**不会**带停已经起来的 server.py（实测验证过），所以这里可以放心清理；
    # 之所以放在 /run 之后而不是等就绪之后，是因为任务残留会让下次 /create 变成覆盖，
    # 语义上「一次性」的任务本来就该用完即删。
    _run(["schtasks", "/delete", "/tn", TASK, "/f"], timeout=40)
    return ok


def wait_ready(timeout: float = 20.0) -> dict | None:
    """等端口真正在监听且接口自报 ok。返回 status 字典或 None。"""
    t0 = time.time()
    while time.time() - t0 < timeout:
        time.sleep(1.5)
        if listeners() and status(8).get("ok"):
            return status(15)
    return None


def _write() -> None:
    txt = out.getvalue()
    try:
        print(txt, end="", flush=True)
    except Exception:
        pass
    try:
        with open(os.path.join(HERE, "restart_server.log"), "a", encoding="utf-8") as f:
            f.write(txt + "\n")
    except Exception:
        pass


# ---------------------------------------------------------------- 主流程
def main() -> int:
    force = "--force" in sys.argv
    p("=" * 66)
    p("乖离率策略 · 工具网站重启")
    p("=" * 66)

    p("\n=== 重启前 ===")
    ls = listeners()
    p(f"  监听 :{PORT} 的进程 = {ls or '无'}")
    st = status(12)
    p(f"  接口自报：updating={st.get('updating')} running={st.get('running_tasks')} "
      f"last_date={st.get('last_date')}")

    # ★ 正在跑 20 分钟的全量下载时绝不能重启：进程被杀会让数据写一半。
    #   除非用户明确 --force。
    if not force and (st.get("updating") or (st.get("running_tasks") or [])):
        p("\n!! 有任务正在执行，已拒绝重启。确实要重启请加 --force")
        _write()
        return 1

    p("\n=== 清理 ===")
    if not ls:
        p("  没有进程在监听，无需清理")
    for pid in ls:
        p(f"  kill {pid}（{ls[pid]}）→ {kill(pid)}")
    time.sleep(3)
    left = listeners()
    for pid in left:
        p(f"  强杀残留 {pid} → {kill(pid)}")
    if left:
        time.sleep(2)
        left = listeners()
    p(f"  复查：监听 = {left or '无'}")

    p("\n=== 启动单实例 ===")
    # ★ 启动只有一次机会是不行的：实测出现过「计划任务返回码全 0、包装脚本也跑了，
    #   但 server.py 没绑上端口」的偶发情况（日志里只有包装脚本的头、没有 banner）。
    #   所以改成最多 3 次尝试，每次都以「端口真的在监听 + 接口自报 ok」为准。
    env_note = "　端口 %s:%s" % (HOST, PORT)
    over = [f"{k}={os.environ[k]}" for k in ("BIA_PORT", "BIA_HOST", "BIA_DB_PATH")
            if os.environ.get(k)]
    if over:
        env_note += "　[环境变量覆盖] " + " ".join(over)

    s = None
    for attempt in (1, 2, 3):
        p(f"  ── 第 {attempt}/3 次尝试 ──")
        if not launch_detached(env_note):
            p("  ⚠ 计划任务路径有异常，请检查上面的 schtasks 返回码")
        p("  等待就绪…")
        s = wait_ready(20)
        if s:
            p(f"★ 服务就绪（第 {attempt} 次尝试）")
            for k in ("last_date", "expected_date", "stale", "breadth", "breadth_main",
                      "bias_only_main", "threshold", "threshold_main",
                      "main_board_only"):
                if k in s:
                    p(f"    {k:<16} = {s[k]}")
            break
        if listeners():
            p("  ⚠ 端口在监听但接口未就绪，再等一轮")
        else:
            p("  ⚠ 端口仍未监听")
        time.sleep(2)
    if not s:
        p("  !! 三次都没起来。请看 webapp/server_console.log 末尾；")
        p("     实在不行就双击 webapp\\控制台.bat → 选「启动服务」。")

    time.sleep(2)
    p("\n=== 重启后 ===")
    ls2 = listeners()
    p(f"  监听 :{PORT} 的进程 = {ls2 or '无'}")
    if len(ls2) == 1:
        p(f"✅ 单实例确认：:{PORT} 由 {list(ls2)[0]} 独占")
        rc = 0 if s else 3
    elif not ls2:
        p("❌ 没有任何进程在监听 —— 启动失败")
        rc = 4
    else:
        p(f"⚠️ 有 {len(ls2)} 个进程同时在监听 :{PORT} —— 多实例抢端口")
        rc = 5
    _write()
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
