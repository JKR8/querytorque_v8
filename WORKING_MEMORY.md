 Most recent tpc-ds run:
 
  Ready to validate:
  - Collection: retry_neutrals/ (43 queries)
  - Variants per query: 4 workers (W1, W2, W3, W4)
  - Total optimizations: 43 × 4 = 172
  - Validation method: 5-run trimmed mean (remove min/max, average 3)
  - Expected: 70% improvement rate, biggest win Q88 at 5.25x

Output Location:

  /mnt/c/Users/jakc9/Documents/QueryTorque_V8/research/ado/validation_results/

  Files:
  - postgresql_dsb_validation.json - Final results
    - All 53 queries with speedup measurements
    - Summary stats (wins, passes, regressions, errors)
  - checkpoint.json - Resume checkpoint
    - Saved after each query
    - Deleted on successful completion
    - Allows resuming if interrupted


    best and latest prompt: 
    Written to research/q47_prompt_test.txt — should be visible in your Windows file explorer at
  C:\Users\jakc9\Documents\QueryTorque_V8\research\q47_prompt_test.txt.