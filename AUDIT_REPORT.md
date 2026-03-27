# Stress Audit Report

This pass focused on production-style edge cases without a live Discord or ComfyUI server.

## Scenarios covered

- Concurrent submissions from the same user against parallel-slot limits
- Queue cancellation and daily-limit rollback
- Multi-output delivery when followup webhooks fail/expire
- Queue-position UI churn under bursty enqueue/dequeue activity

## Fixes applied

- Added debounced queue-view refresh scheduling to reduce redundant Discord message edits under load
- Added fallback delivery path for additional outputs: `followup.send -> message.reply -> channel.send`
- Added a local stress audit script at `tools/stress_audit.py`

## Audit result

All scripted scenarios passed in the local stubbed audit harness.
