pattern="$1"
max_attempts=3

for attempt in $(seq 1 "$max_attempts"); do
    pkill -9 -f "$pattern" 2>/dev/null
    sleep 1
    remaining=$(pgrep -f "$pattern" | wc -l)
    if [ "$remaining" -eq 0 ]; then
        echo "OK: no '$pattern' processes remain (attempt $attempt)"
        exit 0
    fi
    echo "WARN: $remaining '$pattern' process(es) survived attempt $attempt, retrying..."
done

echo "FAIL: '$pattern' processes still alive after $max_attempts attempts:"
pgrep -af "$pattern"
exit 1
