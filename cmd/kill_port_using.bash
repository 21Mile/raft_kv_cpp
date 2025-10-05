#!/usr/bin/env bash
# 在这里手动填端口（空格分隔）
PORTS=(24010 24011 24012 24013 24014)

set -euo pipefail

get_pids_for_port() {
  # 只匹配 TCP 且 LISTEN 的进程，输出纯 PID
  lsof -t -iTCP:"$1" -sTCP:LISTEN 2>/dev/null | sort -u
}

# 收集占用端口 -> PID 映射
declare -A PORT_PIDS=()
declare -A PID_SEEN=()
NEED_KILL=()

echo "检查端口 ${PORTS[*]} ..."
any_busy=0
for p in "${PORTS[@]}"; do
  pids=$(get_pids_for_port "$p" || true)
  if [[ -n "$pids" ]]; then
    any_busy=1
    echo "  端口 $p 被占用：PID -> $(echo "$pids" | tr '\n' ' ')"
    PORT_PIDS["$p"]="$pids"
    # 去重收集 PID
    while read -r pid; do
      [[ -z "$pid" ]] && continue
      if [[ -z "${PID_SEEN[$pid]:-}" ]]; then
        PID_SEEN[$pid]=1
        NEED_KILL+=("$pid")
      fi
    done <<< "$pids"
  else
    echo "  端口 $p 空闲 ✅"
  fi
done

if [[ $any_busy -eq 0 ]]; then
  exit 0
fi

echo
echo "将终止以下进程："
for pid in "${NEED_KILL[@]}"; do
  # 打印一点上下文信息
  ps -o pid= -o user= -o comm= -o args= -p "$pid" || true
done

echo
echo "发送 SIGTERM..."
for pid in "${NEED_KILL[@]}"; do
  kill -TERM "$pid" 2>/dev/null || true
done

# 给进程一点时间做清理
sleep 1.5

# 仍存活的再 SIGKILL
LEFT=()
for pid in "${NEED_KILL[@]}"; do
  if kill -0 "$pid" 2>/dev/null; then
    LEFT+=("$pid")
  fi
done

if ((${#LEFT[@]})); then
  echo "发送 SIGKILL..."
  for pid in "${LEFT[@]}"; do
    kill -KILL "$pid" 2>/dev/null || true
  done
fi

# 最终验证
echo
echo "验证端口是否已释放："
all_freed=1
for p in "${PORTS[@]}"; do
  if get_pids_for_port "$p" >/dev/null; then
    echo "  端口 $p 仍被占用 ❌"
    all_freed=0
  else
    echo "  端口 $p 已释放 ✅"
  fi
done

exit $((1 - all_freed))
