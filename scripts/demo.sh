#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:8080}"

echo "=========================================="
echo " FlowForge Stage 1 Live Demo"
echo " API Target: ${API_URL}"
echo "=========================================="
echo ""

# 1. Health & Readiness check
echo "--> Checking API readiness..."
READY=$(curl -s "${API_URL}/readyz")
echo "    Readiness response: ${READY}"
echo ""

# 2. Submit an Echo Job
echo "--> 1. Submitting 'echo' job..."
ECHO_RESP=$(curl -s -X POST "${API_URL}/jobs" \
  -H "Content-Type: application/json" \
  -d '{"type": "echo", "payload": {"message": "Hello from FlowForge Stage 1!"}}')
echo "    Response: ${ECHO_RESP}"

JOB_ID=$(echo "${ECHO_RESP}" | grep -o '"id":"[^"]*' | cut -d'"' -f4)
echo "    Created Job ID: ${JOB_ID}"
echo "    Polling until COMPLETED..."

for i in {1..20}; do
  STATUS_RESP=$(curl -s "${API_URL}/jobs/${JOB_ID}")
  STATUS=$(echo "${STATUS_RESP}" | grep -o '"status":"[^"]*' | cut -d'"' -f4)
  echo "    [$(date +%T)] Status: ${STATUS}"
  if [ "${STATUS}" = "COMPLETED" ]; then
    echo "    Result: ${STATUS_RESP}"
    break
  fi
  sleep 1
done
echo ""

# 3. Submit a Sleep Job (3 seconds)
echo "--> 2. Submitting 'sleep' job (3s)..."
SLEEP_RESP=$(curl -s -X POST "${API_URL}/jobs" \
  -H "Content-Type: application/json" \
  -d '{"type": "sleep", "payload": {"seconds": 3}}')
SLEEP_ID=$(echo "${SLEEP_RESP}" | grep -o '"id":"[^"]*' | cut -d'"' -f4)
echo "    Created Job ID: ${SLEEP_ID}"
echo "    Watching state transitions..."

for i in {1..20}; do
  STATUS_RESP=$(curl -s "${API_URL}/jobs/${SLEEP_ID}")
  STATUS=$(echo "${STATUS_RESP}" | grep -o '"status":"[^"]*' | cut -d'"' -f4)
  echo "    [$(date +%T)] Status: ${STATUS}"
  if [ "${STATUS}" = "COMPLETED" ]; then
    echo "    Final Result: ${STATUS_RESP}"
    break
  fi
  sleep 1
done
echo ""

# 4. Submit an Invalid/Failing Job
echo "--> 3. Submitting invalid task to demonstrate failure handling..."
FAIL_RESP=$(curl -s -X POST "${API_URL}/jobs" \
  -H "Content-Type: application/json" \
  -d '{"type": "nonexistent_task", "payload": {"data": 123}}')
FAIL_ID=$(echo "${FAIL_RESP}" | grep -o '"id":"[^"]*' | cut -d'"' -f4)
echo "    Created Job ID: ${FAIL_ID}"
echo "    Watching for FAILED status..."

for i in {1..20}; do
  STATUS_RESP=$(curl -s "${API_URL}/jobs/${FAIL_ID}")
  STATUS=$(echo "${STATUS_RESP}" | grep -o '"status":"[^"]*' | cut -d'"' -f4)
  echo "    [$(date +%T)] Status: ${STATUS}"
  if [ "${STATUS}" = "FAILED" ]; then
    echo "    Failure recorded: ${STATUS_RESP}"
    break
  fi
  sleep 1
done
echo ""

# 5. List all jobs
echo "--> 4. Listing jobs..."
LIST_RESP=$(curl -s "${API_URL}/jobs?limit=5")
echo "    List response: ${LIST_RESP}"
echo ""
echo "=========================================="
echo " Demo completed successfully!"
echo "=========================================="
