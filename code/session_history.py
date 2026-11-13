#!/usr/bin/env python3
"""读取 Telegram 桥的历史原始对话（所有 Codex 会话文件），供换窗/压缩后按需查细节。

这是「三层记忆」里 L3（原始记录档案）的一个示例实现：给 Agent 一个能按关键词
翻原始会话日志的工具，弥补压缩摘要丢细节的问题。示例针对 Codex CLI 的
rollout-*.jsonl 日志格式 + Telegram 桥的消息标记；换成你自己的 Agent 框架时，
改 BASE 路径和筛选规则即可，思路通用。

用法：
  python3 session_history.py                 # 最近 30 条对话
  python3 session_history.py --recent 50     # 最近 50 条
  python3 session_history.py --search 关键词  # 按关键词查（最多 20 条）
  python3 session_history.py --hours 24      # 最近 24 小时内
输出为纯文本，单条截断、总量有上限，避免一次灌入过多 token。
数据目录可用环境变量 SESSION_HISTORY_BASE 覆盖。
"""
import argparse, glob, json, os, re, sys
from datetime import datetime, timezone, timedelta

BASE = os.environ.get("SESSION_HISTORY_BASE",
                      "/var/lib/docker/volumes/codex-wirebot-data/_data")
SESSIONS = os.path.join(BASE, "codex-home", "sessions")
CST = timezone(timedelta(hours=8))
TG_PREFIX = re.compile(r"^\[Telegram消息 · [^\]]+\]\s*", re.S)
MAX_MSG_CHARS = 500
MAX_OUT_CHARS = 12000


def to_cst(ts):
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(CST)


def extract():
    """Return list of (dt, who, text) across all session files, chronological."""
    turns = []
    files = sorted(glob.glob(os.path.join(SESSIONS, "**", "rollout-*.jsonl"), recursive=True))
    for path in files:
        pending_user = None  # (dt, text) waiting for its reply
        try:
            f = open(path, encoding="utf-8")
        except OSError:
            continue
        for line in f:
            try:
                ev = json.loads(line)
            except Exception:
                continue
            ts = ev.get("timestamp")
            if not ts:
                continue
            t = ev.get("type")
            payload = ev.get("payload") or {}
            if t == "response_item" and payload.get("role") == "user":
                text = ""
                for c in payload.get("content") or []:
                    if isinstance(c, dict) and c.get("type") in ("input_text", "text"):
                        text += c.get("text", "")
                if TG_PREFIX.match(text):
                    if pending_user is not None:
                        turns.append((pending_user[0], "月月", pending_user[1]))
                    pending_user = (to_cst(ts), TG_PREFIX.sub("", text).strip())
            elif t == "event_msg" and payload.get("type") == "task_complete":
                reply = (payload.get("last_agent_message") or "").strip()
                if pending_user is not None:
                    turns.append((pending_user[0], "月月", pending_user[1]))
                    if reply:
                        turns.append((to_cst(ts), "哥哥", reply))
                    pending_user = None
        if pending_user is not None:
            turns.append((pending_user[0], "月月", pending_user[1]))
    turns.sort(key=lambda x: x[0])
    return turns


def fmt(turns):
    out = []
    total = 0
    for dt, who, text in turns:
        text = text.replace("\n", " ")
        if len(text) > MAX_MSG_CHARS:
            text = text[:MAX_MSG_CHARS] + "…"
        line = f"[{dt.strftime('%m-%d %H:%M')}] {who}：{text}"
        total += len(line) + 1
        if total > MAX_OUT_CHARS:
            out.append("（已达输出上限，后面的省略；可缩小范围再查）")
            break
        out.append(line)
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--recent", type=int, default=30)
    ap.add_argument("--search", type=str, default="")
    ap.add_argument("--hours", type=float, default=0)
    args = ap.parse_args()

    turns = extract()
    if args.search:
        hit = [t for t in turns if args.search in t[2]]
        turns = hit[-20:]
        print(f"关键词「{args.search}」共 {len(hit)} 条，显示最近 {len(turns)} 条：")
    elif args.hours > 0:
        since = datetime.now(CST) - timedelta(hours=args.hours)
        turns = [t for t in turns if t[0] >= since]
        print(f"最近 {args.hours:g} 小时共 {len(turns)} 条：")
    else:
        turns = turns[-args.recent:]
        print(f"最近 {len(turns)} 条对话：")
    print(fmt(turns))


if __name__ == "__main__":
    main()
