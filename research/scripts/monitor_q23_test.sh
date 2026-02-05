#!/bin/bash
# Monitor Q23 test progress

echo "Q23 TEST MONITOR"
echo "================"
echo ""

# Check if test is running
if ps aux | grep -q "[p]ython3 test_q23_all_modes.py"; then
    echo "✅ Test is RUNNING"
else
    echo "⚠️  Test process not found (may have completed or not started)"
fi

echo ""
echo "Latest output from test:"
echo "------------------------"
tail -50 test_results/q23_full_run.log 2>/dev/null || echo "Log file not found yet"

echo ""
echo "========================"
echo "Commands:"
echo "  tail -f test_results/q23_full_run.log  # Follow live output"
echo "  bash monitor_q23_test.sh                # Run this script again"
echo "========================"
