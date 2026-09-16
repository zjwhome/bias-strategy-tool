# -*- coding: utf-8 -*-
"""把本地 HEAD 完整推送到 GitHub（Git Data API）。

用法：
    python push_to_github.py            # 幂等：远端已是本地 HEAD 时只做核验
前置：Windows 凭据管理器里有 `git:https://github.com`（账号 zjwhome + PAT）。
输出末尾出现 `ALL OK` 才算成功（会同时核对 ref、tree 与全部文件的 blob sha）。

★ 为什么重写（push2.py 的致命缺陷）：
  push2.py 的 FILES 是**硬编码的 3 个文件**，并用 `base_tree = 父提交的 tree`
  叠加这 3 个文件的 blob 来构造 tree。于是：
    · 本次提交改动的另外 5 个文件（db.py / strategy_core.py / updater.py /
      strategy_config.json / 预览PNG）**根本没被推上去**；
    · 生成的 tree 与本地 tree 不同（`tree=a0efda38fa local=964a3e3fd2`），
      但它接着用 `git update-ref` 把本地 main 改成远端 sha —— **等于伪造了一致性**，
      本地 HEAD 反而指向一个本地不存在的对象（`git ls-tree HEAD` 报 not a tree object）。
  ★ 另外 `git rev-parse HEAD:path` 在解析失败时会**把参数原样回显到 stdout**，
    所以 push2 的校验把 "HEAD:webapp/server.py" 当成了本地 blob sha → 必然 MISMATCH。

★ 本脚本的正确做法：
  1. `git ls-tree -r -z HEAD` 取**本地完整 tree**（不再用 base_tree 拼）；
  2. 直接尝试用本地 blob sha 建 tree；若 GitHub 缺对象（422）则逐个 POST /git/blobs；
  3. **断言远端 tree sha 逐字节等于本地 `HEAD^{tree}`**，不等就中止（不写 ref）；
  4. 只有断言通过才创建 commit 并更新 ref，最后才对齐本地 ref。
"""
import base64
import ctypes
import ctypes.wintypes as wt
import datetime
import json
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request

ROOT = r"C:\Users\郑嘉伟\WorkBuddy\股票工具"
REPO = "zjwhome/bias-strategy-tool"
BRANCH = "main"


def say(*a):
    print(*a, flush=True)


def git(*args, binary=False, input_bytes=None):
    r = subprocess.run(["git"] + list(args), cwd=ROOT, capture_output=True, input=input_bytes)
    if binary:
        return r.returncode, r.stdout
    return r.returncode, r.stdout.decode("utf-8", "replace")


# ---------------- 凭据：读 Windows 凭据管理器 ----------------
CRED_TYPE_GENERIC = 1


class CREDENTIAL(ctypes.Structure):
    _fields_ = [
        ("Flags", wt.DWORD), ("Type", wt.DWORD),
        ("TargetName", wt.LPWSTR), ("Comment", wt.LPWSTR),
        ("LastWritten", wt.FILETIME), ("CredentialBlobSize", wt.DWORD),
        ("CredentialBlob", ctypes.POINTER(ctypes.c_byte)),
        ("Persist", wt.DWORD), ("AttributeCount", wt.DWORD),
        ("Attributes", ctypes.c_void_p), ("TargetAlias", wt.LPWSTR),
        ("UserName", wt.LPWSTR),
    ]


_adv = ctypes.WinDLL("advapi32", use_last_error=True)
_adv.CredReadW.argtypes = [wt.LPCWSTR, wt.DWORD, wt.DWORD, ctypes.POINTER(ctypes.POINTER(CREDENTIAL))]
_adv.CredReadW.restype = wt.BOOL
_adv.CredFree.argtypes = [ctypes.c_void_p]


def git_cred():
    p = ctypes.POINTER(CREDENTIAL)()
    if not _adv.CredReadW("git:https://github.com", CRED_TYPE_GENERIC, 0, ctypes.byref(p)):
        raise SystemExit("凭据管理器里没有 git:https://github.com")
    try:
        c = p.contents
        blob = ctypes.string_at(c.CredentialBlob, c.CredentialBlobSize).decode("utf-16-le", "replace")
        return c.UserName, blob
    finally:
        _adv.CredFree(p)


def api(method, path, body=None, token="", timeout=90):
    req = urllib.request.Request(
        "https://api.github.com" + path,
        data=(json.dumps(body).encode("utf-8") if body is not None else None),
        method=method)
    req.add_header("Authorization", "Bearer " + token)
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("User-Agent", "wbb-push")
    if body is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")
    except Exception as e:
        return -1, "%s: %s" % (type(e).__name__, e)


# ---------------- 本地对象 ----------------
def ls_tree_full():
    """[(mode, sha, path)] —— 用 -z 分隔，避免中文路径被转义"""
    rc, out = git("ls-tree", "-r", "-z", "HEAD")
    items = []
    for rec in out.split("\0"):
        if not rec.strip():
            continue
        meta, _, path = rec.partition("\t")
        parts = meta.split()
        if len(parts) != 3:
            continue
        mode, typ, sha = parts
        if typ != "blob":
            continue
        items.append((mode, sha, path))
    return items


def head_meta():
    fmt = "%an%x00%ae%x00%at%x00%cn%x00%ce%x00%ct%x00%P%x00%B"
    rc, out = git("log", "-1", "--format=" + fmt)
    p = out.split("\x00")
    return dict(author_name=p[0], author_email=p[1], author_ts=p[2],
                committer_name=p[3], committer_email=p[4], committer_ts=p[5],
                parents=p[6].split(), message=p[7])


def iso(ts):
    return datetime.datetime.fromtimestamp(
        int(ts), datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------- 构建远端 tree ----------------
def build_tree(token, items):
    """优先直接用本地 blob sha（GitHub 极可能已有）；422 则逐个上传后再建。"""
    tree = [{"path": path, "mode": mode, "type": "blob", "sha": sha}
            for mode, sha, path in items]
    st, r = api("POST", "/repos/%s/git/trees" % REPO,
                {"tree": tree}, token=token)
    if st == 201:
        say("  tree 直连本地 blob 成功（0 次上传）")
        return r["sha"]

    say("  直连失败（HTTP %s），改为逐个上传 blob…" % st)
    new_tree = []
    up = 0
    for mode, sha, path in items:
        rc, raw = git("cat-file", "blob", sha, binary=True)
        if rc != 0:
            raise SystemExit("读不到本地 blob %s (%s)" % (sha[:10], path))
        st2, r2 = api("POST", "/repos/%s/git/blobs" % REPO,
                      {"content": base64.b64encode(raw).decode("ascii"),
                       "encoding": "base64"}, token=token)
        if st2 not in (201, 200):
            raise SystemExit("上传 blob 失败 %s %s: %s" % (st2, path, str(r2)[:200]))
        if r2["sha"] != sha:
            say("    ⚠️ %s blob sha 不一致：本地 %s 远端 %s" % (path, sha[:10], r2["sha"][:10]))
        new_tree.append({"path": path, "mode": mode, "type": "blob", "sha": r2["sha"]})
        up += 1
    say("  已上传 %d 个 blob" % up)
    st3, r3 = api("POST", "/repos/%s/git/trees" % REPO, {"tree": new_tree}, token=token)
    if st3 != 201:
        raise SystemExit("建 tree 失败 %s: %s" % (st3, str(r3)[:300]))
    return r3["sha"]


# ---------------- 对齐本地 ref ----------------
def align(remote_sha, tree, parents, cm):
    def sig(who, tz):
        # cm[..._ts] 是 `git log --format=%at/%ct` 给的 unix 秒（UTC）
        return "%s <%s> %s %s" % (cm[who + "_name"], cm[who + "_email"],
                                  cm[who + "_ts"], tz)

    body = cm["message"]
    variants = {"raw": body,
                "+\\n": body.rstrip("\n") + "\n",
                "+\\n\\n": body.rstrip("\n") + "\n\n",
                "strip": body.rstrip("\n")}
    tries = 0
    for tz in ("+0800", "+0000"):
        head = "tree %s\n" % tree + "".join("parent %s\n" % p for p in parents)
        head += "author %s\ncommitter %s\n\n" % (sig("author", tz), sig("committer", tz))
        for mname, m in variants.items():
            tries += 1
            rc, out = git("hash-object", "-t", "commit", "-w", "--stdin",
                          input_bytes=(head + m).encode("utf-8"))
            if out.strip() == remote_sha:
                say("  ALIGNED via tz=%s msg=%s (%d tries)" % (tz, mname, tries))
                git("update-ref", "refs/heads/%s" % BRANCH, remote_sha)
                return True
    say("  !! 未对齐（试了 %d 种）→ 远端已推成功，本地保持原样（下次会再推一次）" % tries)
    return False


# ---------------- 核验 ----------------
def verify(token, local):
    say("--- verify ---")
    ok = True
    st, ref = api("GET", "/repos/%s/git/ref/heads/%s" % (REPO, BRANCH), token=token)
    if st != 200:
        say("GET ref failed", st, str(ref)[:300])
        return False
    remote = ref["object"]["sha"]
    say("remote %s = %s" % (BRANCH, remote))
    say("local  HEAD = %s" % local)
    same_ref = (remote == local)
    say("ref match   : %s" % same_ref)
    ok = ok and same_ref

    # 逐文件比对（本地用 cat-file，**不用 rev-parse**：它失败时会回显参数）
    rc, local_tree = git("rev-parse", "HEAD^{tree}")
    st, rt = api("GET", "/repos/%s/git/trees/%s" % (REPO, local_tree.strip()), token=token)
    if st == 200:
        say("tree 存在且与本地一致: %s" % local_tree.strip()[:10])
    else:
        say("!! 远端找不到本地 tree %s（HTTP %s）→ 内容并未完整同步"
            % (local_tree.strip()[:10], st))
        ok = False
        return ok

    items = ls_tree_full()
    mism = 0
    for mode, sha, path in items:
        # ⚠️ 中文路径必须 percent-encode，否则 urllib 直接抛 UnicodeEncodeError（返回 -1）
        q = urllib.parse.quote(path)
        st, c = api("GET", "/repos/%s/contents/%s?ref=%s" % (REPO, q, remote), token=token)
        rsha = c.get("sha") if st == 200 else ("HTTP%d" % st)
        good = (rsha == sha)
        if not good:
            mism += 1
            say("  MISMATCH %-40s remote=%s local=%s" % (path, str(rsha)[:10], sha[:10]))
    say("  文件比对：%d 个文件，%d 个不一致" % (len(items), mism))
    ok = ok and (mism == 0)

    if ok:
        st, cm = api("GET", "/repos/%s/git/commits/%s" % (REPO, remote), token=token)
        if st == 200:
            say("msg : %s" % cm["message"].splitlines()[0])
            say("date: %s" % cm["committer"]["date"])
    say("ALL OK" if ok else "!! NOT IN SYNC")
    return ok


def main():
    user, token = git_cred()
    say("cred user=%s token_len=%d" % (user, len(token)))

    rc, local = git("rev-parse", "HEAD")
    local = local.strip()
    rc, parent = git("rev-parse", "HEAD^")
    parent = parent.strip()
    rc, tree = git("rev-parse", "HEAD^{tree}")
    tree = tree.strip()
    cm = head_meta()
    items = ls_tree_full()
    say("local head=%s parent=%s tree=%s" % (local[:10], parent[:10], tree[:10]))
    say("文件数（本地完整 tree）=%d" % len(items))

    st, ref = api("GET", "/repos/%s/git/ref/heads/%s" % (REPO, BRANCH), token=token)
    remote_head = ref["object"]["sha"] if st == 200 else ""
    say("remote %s = %s" % (BRANCH, remote_head[:10]))

    if remote_head == local:
        say("远端已是本地 HEAD，跳过推送")
        return 0 if verify(token, local) else 7

    say("\n--- 构造完整 tree ---")
    nt = build_tree(token, items)
    say("  tree 远端=%s 本地=%s  %s"
        % (nt[:10], tree[:10], "OK" if nt == tree else "!! 不同"))
    if nt != tree:
        say("!! tree 不一致 → 中止，不写 ref（避免再产生一个内容残缺的提交）")
        return 5

    say("\n--- 创建 commit ---")
    body = {"message": cm["message"], "tree": nt, "parents": [parent],
            "author": {"name": cm["author_name"], "email": cm["author_email"],
                       "date": iso(cm["author_ts"])},
            "committer": {"name": cm["committer_name"], "email": cm["committer_email"],
                          "date": iso(cm["committer_ts"])}}
    st, r = api("POST", "/repos/%s/git/commits" % REPO, body, token=token)
    if st != 201:
        say("建 commit 失败 %s: %s" % (st, str(r)[:300]))
        return 6
    new_sha = r["sha"]
    say("  remote commit=%s（GitHub 会改写时区/message，随后对齐）" % new_sha[:10])

    say("\n--- 更新 ref（force，覆盖上一版残缺提交）---")
    st, r = api("PATCH", "/repos/%s/git/refs/heads/%s" % (REPO, BRANCH),
                {"sha": new_sha, "force": True}, token=token)
    say("  PATCH ref -> %s" % st)
    if st not in (200, 201):
        say("  更新 ref 失败：%s" % str(r)[:300])
        return 6

    say("\n--- 对齐本地 ref ---")
    align(new_sha, nt, [parent], cm)

    return 0 if verify(token, git("rev-parse", "HEAD")[1].strip()) else 7


sys.exit(main())
