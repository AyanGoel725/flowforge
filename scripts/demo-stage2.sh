#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:8080}"

echo "=========================================="
echo " FlowForge Stage 2 Reliability & Retry Demo"
echo " API Target: $API_URL"
echo "=========================================="
echo ""

# 1. Health & Readiness check
echo "--> Checking API readiness..."
curl -s "$API_URL/readyz" | grep -o '"status":"[^"]*"' || true
echo ""

# 2. Submit a Flaky Job
echo "--> 1. Submitting 'flaky' task (fails 2x, succeeds on attempt 3)..."
FLAKY_RESP=$(curl -s -X POST "$API_URL/jobs" \
  -H "Content-Type: application/json" \
  -d '{"type":"flaky","payload":{},"max_attempts":3}')
FLAKY_ID=$(echo "$FLAKY_RESP" | grep -o '"id":"[^"]*' | cut -d'"' -f4)
echo "    Created Job ID: $FLAKY_ID"

for i in $(seq 1 30); do
  sleep 1
  STATUS_RESP=$(curl -s "$API_URL/jobs/$FLAKY_ID")
  STATUS=$(echo "$STATUS_RESP" | grep -o '"status":"[^"]*' | cut -d'"' -f4)
  ATTEMPTS=$(echo "$STATUS_RESP" | grep -o '"attempt_count":[0-9]*' | cut -d':' -f2)
  echo "    [$(date +%T)] Status: $STATUS | Attempts: $ATTEMPTS"
  if [ "$STATUS" = "COMPLETED" ]; then
    echo "    Flaky Job succeeded on attempt $ATTEMPTS!"
    break
  fi
done
echo ""

# 3. Submit an Always Fail Job
echo "--> 2. Submitting 'always_fail' task (exhausts all 3 attempts)..."
AF_RESP=$(curl -s -X POST "$API_URL/jobs" \
  -H "Content-Type: application/json" \
  -d '{"type":"always_fail","payload":{},"max_attempts":3}')
AF_ID=$(echo "$AF_RESP" | grep -o '"id":"[^"]*' | cut -d'"' -f4)
echo "    Created Job ID: $AF_ID"

for i in $(seq 1 30); do
  sleep 1
  STATUS_RESP=$(curl -s "$API_URL/jobs/$AF_ID")
  STATUS=$(echo "$STATUS_RESP" | grep -o '"status":"[^"]*' | cut -d'"' -f4)
  ATTEMPTS=$(echo "$STATUS_RESP" | grep -o '"attempt_count":[0-9]*' | cut -d':' -f2)
  echo "    [$(date +%T)] Status: $STATUS | Attempts: $ATTEMPTS"
  if [ "$STATUS" = "FAILED" ]; then
    echo "    Job exhausted retries and transitioned to FAILED as expected."
    break
  fi
done
echo ""

# 4. Submit a Permanent Fail Job
echo "--> 3. Submitting 'permanent_fail' task (non-retryable fast-path)..."
PF_RESP=$(curl -s -X POST "$API_URL/jobs" \
  -H "Content-Type: application/json" \
  -d '{"type":"permanent_fail","payload":{},"max_attempts":5}')
PF_ID=$(echo "$PF_RESP" | grep -o '"id":"[^"]*' | cut -d'"' -f4)
echo "    Created Job ID: $PF_ID"

for i in $(seq 1 15); do
  sleep 1
  STATUS_RESP=$(curl -s "$API_URL/jobs/$PF_ID")
  STATUS=$(echo "$STATUS_RESP" | grep -o '"status":"[^"]*' | cut -d'"' -f4)
  ATTEMPTS=$(echo "$STATUS_RESP" | grep -o '"attempt_count":[0-9]*' | cut -d':' -f2)
  echo "    [$(date +%T)] Status: $STATUS | Attempts: $ATTEMPTS"
  if [ "$STATUS" = "FAILED" ]; then
    echo "    Job failed immediately on attempt $ATTEMPTS without retrying."
    break
  fi
done
echo ""

echo "=========================================="
echo " Stage 2 Demo completed successfully!"
echo "=========================================="
