# -*- coding: utf-8 -*-
"""
================================================================================
乖离率策略 · 控制台 (control.py)
================================================================================
这是工具网站的「外部操作界面」——双击桌面的快捷方式即可打开。

功能：
  · 启动 / 关闭 工具网站
  · 查看网站状态、数据日期、今日广度
  · 立即执行 盘前 / 盘中 / 盘后 任务
  · 打开使用手册、数据文件夹
  · 创建 / 修复桌面快捷方式
================================================================================
"""
from __future__ import annotations

import json
import os
import socket
import struct
import subprocess
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
PROJ = os.path.dirname(HERE)
PORT = 8000
HOST = "127.0.0.1"
URL = f"http://{HOST}:{PORT}"
PY = sys.executable
IS_WIN = os.name == "nt"

# ---------------------------------------------------------------- 终端颜色
try:
    os.system("")          # 打开 Windows 控制台 ANSI 支持
except Exception:
    pass
C = dict(r="\033[31m", g="\033[32m", y="\033[33m", b="\033[34m",
         c="\033[36m", w="\033[97m", dim="\033[2m", bd="\033[1m", x="\033[0m")
CLR = all(k in C for k in "rgybcwx")


def col(s: str, c: str) -> str:
    return f"{C[c]}{s}{C['x']}" if CLR else s


def line(ch="─", n=70):
    print(col(ch * n, "dim"))


# ---------------------------------------------------------------- 状态检测
def _run(cmd: list[str], timeout: int = 20):
    """执行外部命令，返回 (code, stdout, stderr)。

    ⚠️ Windows 命令行工具（netstat/taskkill/powershell）输出是 GBK/OEM 编码，
    直接 text=True 用 utf-8 解码会在读取线程里抛 UnicodeDecodeError，
    所以这里统一按字节捕获再做多编码尝试解码。
    """
    try:
        r = subprocess.run(cmd, capture_output=True, timeout=timeout)
    except Exception as e:
        return -1, "", str(e)
    def dec(b: bytes) -> str:
        for enc in ("utf-8", "gbk", "latin-1"):
            try:
                return b.decode(enc)
            except Exception:
                continue
        return (b or b"").decode("utf-8", "replace")
    return r.returncode, dec(r.stdout or b""), dec(r.stderr or b"")


def server_pid() -> int | None:
    """找监听 8000 端口的进程号"""
    if not IS_WIN:
        return None
    code, out, _ = _run(["netstat", "-ano", "-p", "TCP"], timeout=15)
    for ln in out.splitlines():
        p = ln.split()
        if len(p) >= 5 and p[3] == "LISTENING" and p[1].endswith(f":{PORT}"):
            try:
                return int(p[4])
            except Exception:
                continue
    return None


def http_status() -> dict | None:
    """调网站接口拿状态；网站没开则返回 None"""
    try:
        with urllib.request.urlopen(f"{URL}/api/status", timeout=3) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception:
        return None


def port_open() -> bool:
    s = socket.socket()
    s.settimeout(1)
    try:
        s.connect((HOST, PORT))
        return True
    except Exception:
        return False
    finally:
        s.close()


def current_state() -> tuple[bool, dict | None, int | None]:
    pid = server_pid()
    running = port_open() or bool(pid)
    st = http_status() if running else None
    return running, st, pid


# ---------------------------------------------------------------- 启停
def start_server(open_browser: bool = True) -> bool:
    running, st, pid = current_state()
    if running:
        print(col("  ⚠ 工具网站已经在运行了（端口 8000 被占用）。", "y"))
        if pid:
            print(f"     进程号：{pid}")
        if open_browser:
            open_url(URL)
        return True

    print("  正在启动工具网站…")
    logf = open(os.path.join(HERE, "server_console.log"), "a", encoding="utf-8")
    flags = subprocess.CREATE_NEW_CONSOLE if IS_WIN else 0
    subprocess.Popen([PY, "server.py"], cwd=HERE, stdout=logf, stderr=logf,
                     creationflags=flags)

    # 等服务就绪（最多 30 秒）
    for i in range(60):
        time.sleep(0.5)
        if http_status():
            print(col(f"  ✔ 启动成功！地址：{URL}", "g"))
            if open_browser:
                open_url(URL)
            return True
    print(col("  ✘ 启动超时。请检查 server_console.log，或确认 Python 环境是否正常。", "r"))
    return False


def stop_server() -> bool:
    running, st, pid = current_state()
    if not running:
        print(col("  工具网站当前没有运行。", "y"))
        return True
    if not pid:
        print(col("  ✘ 找不到监听 8000 端口的进程，请手动关闭。", "r"))
        return False
    code, out, err = _run(["taskkill", "/F", "/PID", str(pid)], timeout=20)
    if code != 0:
        print(col(f"  ✘ 关闭失败：{(err or out).strip()[:200]}", "r"))
        return False
    for _ in range(20):
        time.sleep(0.3)
        if not port_open():
            print(col(f"  ✔ 已关闭工具网站（进程 {pid}）。", "g"))
            return True
    print(col("  ⚠ 已发送关闭指令，但端口仍被占用，请稍后再试。", "y"))
    return False


def open_url(url: str):
    try:
        if IS_WIN:
            os.startfile(url)
        else:
            subprocess.Popen(["open" if sys.platform == "darwin" else "xdg-open", url])
    except Exception as e:
        print(col(f"  ✘ 打开浏览器失败：{e}", "r"))


def open_path(p: str):
    try:
        if os.path.exists(p):
            os.startfile(p) if IS_WIN else subprocess.Popen(["xdg-open", p])
        else:
            print(col(f"  ✘ 路径不存在：{p}", "r"))
    except Exception as e:
        print(col(f"  ✘ 打开失败：{e}", "r"))


# ---------------------------------------------------------------- 任务
def run_task_cli(key: str):
    sys.path.insert(0, HERE)
    try:
        import db, tasks
        db.init_db()
    except Exception as e:
        print(col(f"  ✘ 载入任务模块失败：{e}", "r"))
        return
    info = tasks.TASK_DEFS[key]
    print()
    print(col(f"  ▶ 开始执行「{info['name']}」", "bd"))
    if key == "postmarket":
        print(col("     该任务会下载全市场数据，约需 20~25 分钟，请耐心等待。", "y"))
    line()
    try:
        res = tasks.run_task(key, "manual", log=lambda s: print("  " + str(s), flush=True))
    except Exception as e:
        print(col(f"  ✘ 执行失败：{e}", "r"))
        return
    line()
    print(col(f"  结论：{res.get('headline','')}", "bd"))
    det = res.get("detail", "")
    if det:
        print()
        print(det)
    print(col(f"  耗时 {res.get('elapsed_sec', 0)} 秒", "dim"))


def task_menu():
    while True:
        print()
        line("=")
        print(col("   立即执行任务", "bd"))
        line("=")
        print("   [1] 盘前任务　（几秒完成，告诉你今天买/卖/还是不动）")
        print("   [2] 盘中任务　（约 1 秒，实时预警持仓止盈止损）")
        print("   [3] 盘后任务　（20~25 分钟，更新全市场数据并算今日广度）")
        print("   [0] 返回上级菜单")
        line()
        ch = input("  请输入序号后回车：").strip()
        if ch == "1":
            run_task_cli("premarket")
        elif ch == "2":
            run_task_cli("intraday")
        elif ch == "3":
            run_task_cli("postmarket")
        elif ch == "0":
            return
        else:
            print(col("  请输入 0~3。", "y"))
        input(col("\n  按回车键继续…", "dim"))


# ---------------------------------------------------------------- 桌面快捷方式
def _make_ico(path: str) -> bool:
    """生成一个 32x32 的 .ico 图标（不需要 Pillow）"""
    try:
        W = H = 32
        BG = (0x2F, 0x6B, 0xFF)      # 蓝
        WH = (0xFF, 0xFF, 0xFF)      # 白
        RED = (0xD9, 0x2B, 0x2B)     # 红（A股涨=红）
        GRN = (0x12, 0xA0, 0x5C)     # 绿

        px = [[(0, 0, 0, 0) for _ in range(W)] for _ in range(H)]
        for y in range(H):
            for x in range(W):
                # 圆角矩形
                r = 6
                inside = True
                cx = min(x, W - 1 - x)
                cy = min(y, H - 1 - y)
                if cx < r and cy < r:
                    if (r - cx) ** 2 + (r - cy) ** 2 > r * r:
                        inside = False
                px[y][x] = (*BG, 255) if inside else (0, 0, 0, 0)

        def rect(x0, y0, x1, y1, c):
            for y in range(max(0, y0), min(H, y1 + 1)):
                for x in range(max(0, x0), min(W, x1 + 1)):
                    if px[y][x][3]:
                        px[y][x] = (*c, 255)

        # 三根 K 线柱（白）
        rect(7, 17, 9, 25, WH)
        rect(11, 11, 13, 25, WH)
        rect(15, 20, 17, 25, WH)
        rect(19, 7, 21, 25, WH)
        # 红色下行折线（跌=绿、涨=红，此处用红点表示信号）
        rect(24, 12, 25, 13, RED)
        rect(6, 8, 7, 9, GRN)

        # BITMAPINFOHEADER (height 用双倍)
        bih = struct.pack("<IiiHHIIiiII", 40, W, H * 2, 1, 32, 0, 0, 0, 0, 0, 0)
        xor = b""
        for y in range(H - 1, -1, -1):         # 自下而上
            for x in range(W):
                r_, g_, b_, a_ = px[y][x]
                xor += struct.pack("<BBBB", b_, g_, r_, a_)
        and_mask = b"\x00" * (W * H // 8)
        img = bih + xor + and_mask

        icondir = struct.pack("<HHH", 0, 1, 1)
        entry = struct.pack("<BBBBHHII", W, H, 0, 0, 1, 32, len(img), 22)
        with open(path, "wb") as f:
            f.write(icondir + entry + img)
        return True
    except Exception as e:
        print(col(f"  （图标生成失败，将使用默认图标：{e}）", "dim"))
        return False


def _ps(cmd: str, timeout: int = 60):
    code, out, err = _run(["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass",
                           "-Command", cmd], timeout=timeout)
    return code, out, err


def create_shortcut() -> bool:
    """在桌面创建快捷方式「乖离率策略控制台」"""
    desktop = os.path.join(os.environ.get("USERPROFILE", ""), "Desktop")
    if not os.path.isdir(desktop):
        print(col("  ✘ 找不到桌面目录。", "r"))
        return False

    ico = os.path.join(HERE, "app.ico")
    has_ico = _make_ico(ico)

    target = os.path.join(HERE, "控制台.bat")
    if not os.path.exists(target):
        print(col(f"  ✘ 找不到 {target}", "r"))
        return False

    lnk = os.path.join(desktop, "乖离率策略控制台.lnk")
    icon_part = f"$sc.IconLocation = '{ico}'; " if has_ico else ""
    ps = (
        "$ws = New-Object -ComObject WScript.Shell; "
        f"$sc = $ws.CreateShortcut('{lnk}'); "
        f"$sc.TargetPath = '{target}'; "
        f"$sc.WorkingDirectory = '{HERE}'; "
        "$sc.Description = '乖离率策略 本地工具站 控制台'; "
        f"{icon_part}"
        "$sc.WindowStyle = 1; "
        "$sc.Save(); Write-Output 'SAVED'"
    )
    try:
        code, out, err = _ps(ps)
    except Exception as e:
        print(col(f"  ✘ 创建失败：{e}", "r"))
        return False

    if os.path.exists(lnk):
        print(col(f"  ✔ 桌面快捷方式已创建：{lnk}", "g"))
        print(col("     双击桌面的「乖离率策略控制台」即可打开本控制台。", "dim"))
        return True
    print(col("  ✘ 创建失败：", "r") + (err or out or "未知错误")[:300])
    return False


# ---------------------------------------------------------------- 主界面
def show_status():
    running, st, pid = current_state()
    line("=")
    print(col("   乖离率策略 · 控制台", "bd") + col("　（本地运行，不联网上传）", "dim"))
    line("=")
    if running:
        print("   工具网站： " + col("● 运行中", "g") + f"   {col(URL,'c')}"
              + (f"   进程 {pid}" if pid else ""))
    else:
        print("   工具网站： " + col("○ 已停止", "dim"))

    if st:
        d = st.get("last_date") or "—"
        b = st.get("breadth")
        th = st.get("threshold")
        if b is None:
            print("   数据日期： " + str(d) + "　（尚无广度数据）")
        else:
            trig = "达标 ✅" if (b is not None and th and b >= th) else "未达标"
            bc = "r" if (b and th and b >= th) else "dim"
            print(f"   数据日期： {d}　今日广度： " + col(f"{b} 只", bc)
                  + f"（门槛 {th}）→ {trig}")
        print(col(f"   最近更新： {st.get('updated_at','—')}", "dim"))
    else:
        if running:
            print(col("   数据状态： 读取中…（网站刚启动，稍后再看）", "dim"))
        else:
            print(col("   数据状态： 网站未运行，无法读取", "dim"))

    # 定时任务概览
    try:
        sys.path.insert(0, HERE)
        import db, tasks
        db.init_db()
        ov = tasks.get_task_overview()
        on = [t for t in ov if t["enabled"]]
        if on:
            print("   定时任务： " + col("　".join(
                f"{t['name']} {t['at_time']}" for t in on), "y"))
        else:
            print("   定时任务： " + col("全部关闭（网站不会自己定时，需要你到网页上开启）", "dim"))
    except Exception:
        pass
    line()


def main_menu():
    while True:
        os.system("cls" if IS_WIN else "clear")
        show_status()
        print("   [1] 启动工具网站（自动打开浏览器）")
        print("   [2] 关闭工具网站")
        print("   [3] 用浏览器打开工具网站")
        line()
        print("   [4] 立即执行任务（盘前 / 盘中 / 盘后）")
        line()
        print("   [5] 打开使用手册")
        print("   [6] 打开数据文件夹")
        print("   [7] 创建 / 修复桌面快捷方式")
        print("   [0] 退出")
        line()
        ch = input("  请输入序号后回车：").strip()

        if ch == "1":
            start_server(True); pause()
        elif ch == "2":
            stop_server(); pause()
        elif ch == "3":
            open_url(URL); pause()
        elif ch == "4":
            task_menu()
        elif ch == "5":
            p = os.path.join(PROJ, "工具站-使用手册.md")
            open_path(p) if os.path.exists(p) else print(col("  ✘ 手册不存在", "r"))
            pause()
        elif ch == "6":
            open_path(os.path.join(PROJ, "data_long")); pause()
        elif ch == "7":
            create_shortcut(); pause()
        elif ch == "0":
            print(col("\n  再见。\n", "dim"))
            return
        else:
            print(col("  请输入 0~7。", "y")); pause()


def pause():
    input(col("\n  按回车键返回菜单…", "dim"))


if __name__ == "__main__":
    args = sys.argv[1:]
    # 支持命令行直调（供脚本/调试使用）
    if args:
        cmd = args[0]
        if cmd == "status":
            show_status()
        elif cmd == "start":
            start_server("--no-browser" not in args)
        elif cmd == "stop":
            stop_server()
        elif cmd == "shortcut":
            create_shortcut()
        elif cmd == "task":
            run_task_cli(args[1] if len(args) > 1 else "premarket")
        else:
            print("用法：control.py [status|start|stop|shortcut|task <key>]")
        raise SystemExit(0)
    try:
        main_menu()
    except (KeyboardInterrupt, EOFError):
        print(col("\n\n  已退出控制台。\n", "dim"))
