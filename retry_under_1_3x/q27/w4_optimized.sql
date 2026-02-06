WITH single_pass AS (
  SELECT 
    i_item_id,
    s_state,
    GROUPING(i_item_id, s_state) AS grouping_id,
    AVG(agg1) AS agg1_avg,
    AVG(agg2) AS agg2_avg,
    AVG(agg3) AS agg3_avg,
    AVG(agg4) AS agg4_avg
  FROM results
  GROUP BY GROUPING SETS ((i_item_id, s_state), (i_item_id), ())
)
SELECT 
  CASE WHEN BIT_AND(grouping_id, 3) = 0 THEN i_item_id ELSE NULL END AS i_item_id,
  CASE WHEN BIT_AND(grouping_id, 3) = 0 THEN s_state 
       WHEN BIT_AND(grouping_id, 3) = 1 THEN NULL  -- grouping by i_item_id only
       ELSE NULL END AS s_state,  -- overall aggregate
  CASE WHEN BIT_AND(grouping_id, 3) = 0 THEN 0 ELSE 1 END AS g_state,
  agg1_avg AS agg1,
  agg2_avg AS agg2,
  agg3_avg AS agg3,
  agg4_avg AS agg4
FROM single_pass
ORDER BY i_item_id NULLS FIRST, s_state NULLS FIRST
LIMIT 100