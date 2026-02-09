"""
Compile rewrite_sets into optimized SQL script, run both original and optimized,
compare row counts and timings.
"""
import json
import time
import sys
import os

# Add project paths
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import duckdb

DB_PATH = "/mnt/c/Users/jakc9/Documents/QueryTorque_V8/paper/sql/tpcds_sf10_1.duckdb"
ORIGINAL_SQL = "/mnt/c/Users/jakc9/Documents/QueryTorque_V8/paper/sql/everyhousehold_deidentified.sql"

# ── Rewrite sets from LLM ──────────────────────────────────────────────
REWRITE_SETS = json.loads(r'''
{
  "rewrite_sets": [
    {
      "id": "rs_01",
      "target": "tbl_household_segmentation",
      "nodes": {
        "main_query": "SELECT\n    ca_address_sk AS address_id\n    ,case\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'A01' then 'Segment_Type_A01'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'A02' then 'Segment_Type_A02'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'A03' then 'Segment_Type_A03'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'A04' then 'Segment_Type_A04'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'B05' then 'Segment_Type_B05'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'B06' then 'Segment_Type_B06'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'B07' then 'Segment_Type_B07'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'C08' then 'Segment_Type_C08'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'C09' then 'Segment_Type_C09'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'C10' then 'Segment_Type_C10'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'D11' then 'Segment_Type_D11'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'D12' then 'Segment_Type_D12'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'D13' then 'Segment_Type_D13'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'E14' then 'Segment_Type_E14'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'E15' then 'Segment_Type_E15'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'E16' then 'Segment_Type_E16'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'F17' then 'Segment_Type_F17'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'F18' then 'Segment_Type_F18'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'F19' then 'Segment_Type_F19'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'G20' then 'Segment_Type_G20'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'G21' then 'Segment_Type_G21'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'G22' then 'Segment_Type_G22'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'G23' then 'Segment_Type_G23'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'H24' then 'Segment_Type_H24'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'H25' then 'Segment_Type_H25'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'H26' then 'Segment_Type_H26'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'H27' then 'Segment_Type_H27'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'I28' then 'Segment_Type_I28'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'I29' then 'Segment_Type_I29'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'I30' then 'Segment_Type_I30'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'J31' then 'Segment_Type_J31'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'J32' then 'Segment_Type_J32'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'J33' then 'Segment_Type_J33'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'J34' then 'Segment_Type_J34'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'K35' then 'Segment_Type_K35'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'K36' then 'Segment_Type_K36'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'K37' then 'Segment_Type_K37'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'L38' then 'Segment_Type_L38'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'L39' then 'Segment_Type_L39'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'L40' then 'Segment_Type_L40'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'L41' then 'Segment_Type_L41'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'L42' then 'Segment_Type_L42'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'M43' then 'Segment_Type_M43'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'M44' then 'Segment_Type_M44'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'M45' then 'Segment_Type_M45'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'M46' then 'Segment_Type_M46'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'M47' then 'Segment_Type_M47'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'N48' then 'Segment_Type_N48'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'N49' then 'Segment_Type_N49'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'N50' then 'Segment_Type_N50'\n        when (substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0')) = 'N51' then 'Segment_Type_N51'\n    end as segment_type\n    ,case\n        when substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) = 'A' then 'Segment_Group_A'\n        when substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) = 'B' then 'Segment_Group_B'\n        when substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) = 'C' then 'Segment_Group_C'\n        when substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) = 'D' then 'Segment_Group_D'\n        when substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) = 'E' then 'Segment_Group_E'\n        when substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) = 'F' then 'Segment_Group_F'\n        when substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) = 'G' then 'Segment_Group_G'\n        when substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) = 'H' then 'Segment_Group_H'\n        when substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) = 'I' then 'Segment_Group_I'\n        when substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) = 'J' then 'Segment_Group_J'\n        when substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) = 'K' then 'Segment_Group_K'\n        when substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) = 'L' then 'Segment_Group_L'\n        when substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) = 'M' then 'Segment_Group_M'\n        when substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) = 'N' then 'Segment_Group_N'\n    end as segment_group\n    ,case\n        when (ca_address_sk % 7 + 1) = 1 then 'Affluence_Level_1'\n        when (ca_address_sk % 7 + 1) = 2 then 'Affluence_Level_2'\n        when (ca_address_sk % 7 + 1) = 3 then 'Affluence_Level_3'\n        when (ca_address_sk % 7 + 1) = 4 then 'Affluence_Level_4'\n        when (ca_address_sk % 7 + 1) = 5 then 'Affluence_Level_5'\n        when (ca_address_sk % 7 + 1) = 6 then 'Affluence_Level_6'\n        when (ca_address_sk % 7 + 1) = 7 then 'Affluence_Level_7'\n    end as affluence\n    ,case\n        when (ca_address_sk % 6 + 1) = 1 then 'Household_Type_Family'\n        when (ca_address_sk % 6 + 1) = 3 then 'Household_Type_Couple'\n        when (ca_address_sk % 6 + 1) = 4 then 'Household_Type_Single_Parent'\n        when (ca_address_sk % 6 + 1) = 5 then 'Household_Type_Single'\n        when (ca_address_sk % 6 + 1) = 6 then 'Household_Type_Group'\n    end as household_composition\n    ,case\n        when (ca_address_sk % 7 + 1) = 1 then 'Income_Bracket_1'\n        when (ca_address_sk % 7 + 1) = 2 then 'Income_Bracket_2'\n        when (ca_address_sk % 7 + 1) = 3 then 'Income_Bracket_3'\n        when (ca_address_sk % 7 + 1) = 4 then 'Income_Bracket_4'\n        when (ca_address_sk % 7 + 1) = 5 then 'Income_Bracket_5'\n        when (ca_address_sk % 7 + 1) = 6 then 'Income_Bracket_6'\n        when (ca_address_sk % 7 + 1) = 7 then 'Income_Bracket_7'\n    end as household_income\n    ,case\n        when (ca_address_sk % 15 + 1) = 1 then 'Age_Range_18_19'\n        when (ca_address_sk % 15 + 1) = 2 then 'Age_Range_20_24'\n        when (ca_address_sk % 15 + 1) = 3 then 'Age_Range_25_29'\n        when (ca_address_sk % 15 + 1) = 4 then 'Age_Range_30_34'\n        when (ca_address_sk % 15 + 1) = 5 then 'Age_Range_35_39'\n        when (ca_address_sk % 15 + 1) = 6 then 'Age_Range_40_44'\n        when (ca_address_sk % 15 + 1) = 7 then 'Age_Range_45_49'\n        when (ca_address_sk % 15 + 1) = 8 then 'Age_Range_50_54'\n        when (ca_address_sk % 15 + 1) = 9 then 'Age_Range_55_59'\n        when (ca_address_sk % 15 + 1) = 10 then 'Age_Range_60_64'\n        when (ca_address_sk % 15 + 1) = 11 then 'Age_Range_65_69'\n        when (ca_address_sk % 15 + 1) = 12 then 'Age_Range_70_74'\n        when (ca_address_sk % 15 + 1) = 13 then 'Age_Range_75_79'\n        when (ca_address_sk % 15 + 1) = 14 then 'Age_Range_80_84'\n        when (ca_address_sk % 15 + 1) = 15 then 'Age_Range_85_plus'\n    end as head_of_household_age\n    ,case\n        when (ca_address_sk % 10 + 1) = 1 then 'Lifestage_1'\n        when (ca_address_sk % 10 + 1) = 2 then 'Lifestage_2'\n        when (ca_address_sk % 10 + 1) = 3 then 'Lifestage_3'\n        when (ca_address_sk % 10 + 1) = 4 then 'Lifestage_4'\n        when (ca_address_sk % 10 + 1) = 5 then 'Lifestage_5'\n        when (ca_address_sk % 10 + 1) = 6 then 'Lifestage_6'\n        when (ca_address_sk % 10 + 1) = 7 then 'Lifestage_7'\n        when (ca_address_sk % 10 + 1) = 8 then 'Lifestage_8'\n        when (ca_address_sk % 10 + 1) = 9 then 'Lifestage_9'\n        when (ca_address_sk % 10 + 1) = 10 then 'Lifestage_10'\n    end as household_lifestage\n    ,case\n        when (ca_address_sk % 10 + 1) = 1 then 'Probability_Level_1'\n        when (ca_address_sk % 10 + 1) = 2 then 'Probability_Level_2'\n        when (ca_address_sk % 10 + 1) = 3 then 'Probability_Level_3'\n        when (ca_address_sk % 10 + 1) = 4 then 'Probability_Level_4'\n        when (ca_address_sk % 10 + 1) = 5 then 'Probability_Level_5'\n        when (ca_address_sk % 10 + 1) = 6 then 'Probability_Level_6'\n        when (ca_address_sk % 10 + 1) = 7 then 'Probability_Level_7'\n        when (ca_address_sk % 10 + 1) = 8 then 'Probability_Level_8'\n        when (ca_address_sk % 10 + 1) = 9 then 'Probability_Level_9'\n        when (ca_address_sk % 10 + 1) = 10 then 'Probability_Level_10'\n    end as child_young_probability\n    ,case\n        when (ca_address_sk % 10 + 1) = 1 then 'Probability_Level_1'\n        when (ca_address_sk % 10 + 1) = 2 then 'Probability_Level_2'\n        when (ca_address_sk % 10 + 1) = 3 then 'Probability_Level_3'\n        when (ca_address_sk % 10 + 1) = 4 then 'Probability_Level_4'\n        when (ca_address_sk % 10 + 1) = 5 then 'Probability_Level_5'\n        when (ca_address_sk % 10 + 1) = 6 then 'Probability_Level_6'\n        when (ca_address_sk % 10 + 1) = 7 then 'Probability_Level_7'\n        when (ca_address_sk % 10 + 1) = 8 then 'Probability_Level_8'\n        when (ca_address_sk % 10 + 1) = 9 then 'Probability_Level_9'\n        when (ca_address_sk % 10 + 1) = 10 then 'Probability_Level_10'\n    end as child_teen_probability\nFROM customer\nJOIN customer_address ON c_current_addr_sk = ca_address_sk"
      },
      "data_flow": "main_query"
    },
    {
      "id": "rs_02",
      "target": "tbl_tech_transition_history",
      "nodes": {
        "dataset": "select\n        customer_id\n        , case\n            when technology_type = 'Technology_A' then 'Tech_Category_A'\n            when technology_type = 'Technology_B' then 'Tech_Category_B'\n            when technology_type = 'Technology_C' then 'Tech_Category_C'\n            else technology_type\n            end as technology_type\n        , location_id\n        , min(calendar_date) as min_date\n        , max(calendar_date) as max_date\n    from broadband_service_daily\n    where calendar_date >= current_date - interval '180 day'\n    group by\n        customer_id\n        ,case\n            when technology_type = 'Technology_A' then 'Tech_Category_A'\n            when technology_type = 'Technology_B' then 'Tech_Category_B'\n            when technology_type = 'Technology_C' then 'Tech_Category_C'\n            else technology_type\n            end\n        , location_id",
        "main_query": "SELECT\n    count (distinct a.customer_id) as customer_count\n    , a.technology_type as previous_technology\n    , b.technology_type as new_technology\n    ,segment_type\n    , segment_group\n    , affluence\n    , household_composition\n    , household_income\n    , head_of_household_age\n    , household_lifestage\n    , child_young_probability\n    , child_teen_probability\n    , a.max_date + ((13 - (extract('dow' from a.max_date))) % 7) * interval '1 day' as transition_date\nfrom dataset a\ninner join dataset b\n    on a.location_id = b.location_id\n    and a.max_date between b.min_date - interval '7 day' and b.min_date + interval '7 day'\n    and a.technology_type <> b.technology_type\nLEFT JOIN tbl_household_segmentation c\n    on a.location_id = c.address_id\ngroup BY\n     a.technology_type\n    , b.technology_type\n    ,segment_type\n    , segment_group\n    , affluence\n    , household_composition\n    , household_income\n    , head_of_household_age\n    , household_lifestage\n    , child_young_probability\n    , child_teen_probability\n    , a.max_date + ((13 - (extract('dow' from a.max_date))) % 7) * interval '1 day'"
      },
      "data_flow": "dataset -> main_query"
    },
    {
      "id": "rs_03",
      "target": "tbl_address_portfolio_v1",
      "nodes": {
        "broadband_canvas": "select\n        a.customer_id\n        ,a.service_id\n        ,a.product_code\n        ,a.product_name\n        ,CASE WHEN a.location_id is not null then a.technology_type else null end as service_technology\n        , a.location_id\n        , a.location_id as address_id\n        , a.customer_type\n        , CASE\n            WHEN b.plan_group ='Plan_Group_Standard' then 'Plan_Tier_Standard'\n            WHEN b.plan_group ='Plan_Group_Basic' then 'Plan_Tier_Basic'\n            WHEN b.plan_group ='Plan_Group_Advanced' then 'Plan_Tier_Advanced'\n            WHEN b.plan_group ='Plan_Group_Premium' then 'Plan_Tier_Premium'\n            WHEN b.plan_group ='Plan_Group_High_Speed' then 'Plan_Tier_High_Speed'\n            when product_code = 'Plan_020' THEN\n                            CASE\n                                when speed_class in ('Speed_High','Speed_High_Plus') then 'Plan_Tier_Ultra'\n                                when speed_class = 'Speed_Very_High' then 'Plan_Tier_Premium'\n                                else 'Plan_Tier_Elite'\n                            end\n            when product_code = 'Plan_Code_001' then 'Plan_Tier_Ultra'\n            when product_code = 'Plan_Code_002' then 'Plan_Tier_Premium'\n            WHEN b.plan_group ='Plan_Group_Elite' then 'Plan_Tier_Elite'\n            WHEN b.plan_group ='Plan_Group_Standard_Voice'  THEN 'Plan_Tier_Voice'\n            WHEN b.plan_group in ('Plan_Group_Bundled','Plan_Group_Family','Plan_Group_Trial','Plan_Group_Business','Plan_Group_Data') THEN 'Plan_Tier_Other'\n            ELSE 'Plan_Tier_Other'\n          end as plan_parent\nfrom broadband_service_daily a\nLEFT JOIN plan_mapping b\non a.product_name = b.product_name\nwhere calendar_date = (select max(calendar_date) from broadband_service_daily)\nand customer_type in ('Business','Consumer')\nand a.location_id is not null\ngroup by\n a.customer_id\n        ,a.service_id\n        ,a.product_code\n        ,a.product_name\n        , a.technology_type\n        , a.location_id\n        , a.location_id\n        , a.customer_type\n        , CASE\n            WHEN b.plan_group ='Plan_Group_Standard' then 'Plan_Tier_Standard'\n            WHEN b.plan_group ='Plan_Group_Basic' then 'Plan_Tier_Basic'\n            WHEN b.plan_group ='Plan_Group_Advanced' then 'Plan_Tier_Advanced'\n            WHEN b.plan_group ='Plan_Group_Premium' then 'Plan_Tier_Premium'\n            WHEN b.plan_group ='Plan_Group_High_Speed' then 'Plan_Tier_High_Speed'\n            when product_code = 'Plan_020' THEN\n                            CASE\n                                when speed_class in ('Speed_High','Speed_High_Plus') then 'Plan_Tier_Ultra'\n                                when speed_class = 'Speed_Very_High' then 'Plan_Tier_Premium'\n                                else 'Plan_Tier_Elite'\n                            end\n            when product_code = 'Plan_Code_001' then 'Plan_Tier_Ultra'\n            when product_code = 'Plan_Code_002' then 'Plan_Tier_Premium'\n            WHEN b.plan_group ='Plan_Group_Elite' then 'Plan_Tier_Elite'\n            WHEN b.plan_group ='Plan_Group_Standard_Voice'  THEN 'Plan_Tier_Voice'\n            WHEN b.plan_group in ('Plan_Group_Bundled','Plan_Group_Family','Plan_Group_Trial','Plan_Group_Business','Plan_Group_Data') THEN 'Plan_Tier_Other'\n            ELSE 'Plan_Tier_Other'\n          end",
        "mobile_canvas": "select\n          customer_id\n          ,service_id\n          ,customer_type\n          ,case when service_category = 'Data' then 'Mobile_Broadband' else 'Mobile_Voice' end as plan_type_category\n          ,case\n            when product_code in ('Product_001','Product_002','Product_003','Product_004','Product_005','Product_006') then 'Plan_Tier_Starter'\n            when product_code in ('Product_007','Product_008','Product_009','Product_010','Product_011','Product_012') then 'Plan_Tier_Basic'\n            when product_code in ('Product_013','Product_014','Product_015','Product_016','Product_017','Product_018') then 'Plan_Tier_Advanced'\n            when product_code in ('Product_019','Product_020','Product_021','Product_022','Product_023','Product_024') then 'Plan_Tier_Premium'\n            WHEN product_code IN('Product_025','Product_026') THEN 'Plan_Tier_Bundle'\n            when product_code in ('Product_027','Product_028') then 'Plan_Tier_Extra_Small'\n            when product_code in ('Product_029','Product_030') then 'Plan_Tier_Small'\n            when product_code in ('Product_031','Product_032','Product_033','Product_034') then 'Plan_Tier_Medium'\n            when product_code in ('Product_035','Product_036','Product_037','Product_038') then 'Plan_Tier_Large'\n            WHEN product_code IN('Product_039','Product_040') THEN 'Plan_Tier_Extra_Large'\n            WHEN service_category = 'Data' THEN 'Plan_Tier_Mobile_Data'\n            else 'Plan_Tier_Mobile_Other'\n          end as plan_parent\n    from mobile_service_daily m\n          where calendar_date in (Select Max(calendar_date) from mobile_service_daily )\n        and customer_type in ('Consumer','Business')",
        "prepaid_canvas": "select distinct\n            customer_id\n            ,service_id\n            ,customer_type\n            ,case\n                when service_provider = 'Operator_A' and service_name not like '%Data%' then 'Plan_Tier_Voice_Service'\n                when service_provider = 'Operator_A' and service_name like '%Data%' then 'Plan_Tier_Data_Service'\n                when service_provider = 'Operator_B' and service_name not like '%Data%' then 'Plan_Tier_Alternative_Voice'\n                else 'Plan_Tier_Prepaid_Other'\n            end as plan_parent\n    from prepaid_service_daily\n        where calendar_date = (select max(calendar_date) from prepaid_service_daily)\n        and customer_type in('Consumer','Business')",
        "broadband_active_status": "select\n        ic1.location_id as address_id\n        ,MAX(case when ic1.service_connect_date >= current_date - interval '90 day' then 'New_Customer' end) as new_flag\n        ,MAX(case when ic2.location_id is not null then 'Active_Operator_A' end) as active_flag\n        ,MAX(case when ic2.location_id is null then 'Inactive_Operator_A' end) as inactive_flag\n    from broadband_service_daily ic1\n    left join (\n            select distinct\n                ic2.location_id\n                ,ic2.service_connect_date\n            from broadband_service_daily ic2\n            where calendar_date = (select max(calendar_date) from broadband_service_daily)\n            and customer_type in ('Consumer','Business')\n        ) ic2 on ic1.location_id = ic2.location_id\n    where ic1.calendar_date >= current_date - interval '90 day'\n    and ic1.customer_type in ('Consumer','Business')\n    group by\n        ic1.location_id",
        "service_data_s1": "select customer_id, service_id,customer_type, product_name,service_technology, location_id, plan_parent, null::VARCHAR as plan_type_category, 'Broadband_Service' as source from broadband_canvas\nUNION ALL\nselect customer_id, service_id, customer_type, null, null, null, plan_parent, null, 'Prepaid_Service'  from prepaid_canvas\nUNION ALL\nselect customer_id, service_id, customer_type, null, null, null,plan_parent, plan_type_category, 'Mobile_Service' from mobile_canvas",
        "service_tech": "select distinct service_technology, location_id from service_data_s1",
        "service_data": "select\ncoalesce(a.location_id, b.location_id) as address_id\n,  CASE WHEN coalesce(a.location_id, b.location_id) IS NOT NULL THEN string_agg(c.service_technology, ', ' ORDER BY c.service_technology) ELSE NULL END as fixed_service_technology\n        ,MAX(case when a.source = 'Broadband_Service' then 'Broadband_Service' end) as broadband_service\n        ,MAX(case when a.source = 'Mobile_Service' then 'Mobile_Service' end) as mobile_service\n        ,MAX(case when a.source = 'Prepaid_Service' then 'Prepaid_Service' end) as prepaid_service\n        ,MAX(case when a.plan_type_category = 'Data' then 'Mobile_Broadband' end) as mobile_broadband\n        ,COUNT(DISTINCT case when a.customer_type = 'Consumer' then a.customer_id else null end) as consumer_count\n        ,COUNT(DISTINCT case when a.customer_type = 'Business' then a.customer_id else null end) as business_count\n        ,COUNT(DISTINCT case when a.customer_type = 'Consumer' then a.service_id else null end) as consumer_services\n        ,COUNT(DISTINCT case when a.customer_type = 'Business' then a.service_id else null end) as business_services\n        ,sum(case when a.plan_parent = 'Plan_Tier_Starter' then 1 else 0 end) as service_starter\n        ,sum(case when a.plan_parent = 'Plan_Tier_Basic' then 1 else 0 end) as service_basic\n        ,sum(case when a.plan_parent = 'Plan_Tier_Advanced' then 1 else 0 end) as service_advanced\n        ,sum(case when a.plan_parent = 'Plan_Tier_Premium' then 1 else 0 end) as service_premium\n        ,sum(case when a.plan_parent = 'Plan_Tier_Ultra' then 1 else 0 end) as service_ultra\n        ,sum(case when a.plan_parent = 'Plan_Tier_Elite' then 1 else 0 end) as service_elite\n        ,sum(case when a.plan_parent = 'Plan_Tier_High_Speed' then 1 else 0 end) as service_high_speed\n         ,sum(case when a.plan_parent = 'Plan_Tier_Satellite' then 1 else 0 end) as service_satellite\n        ,sum(case when a.plan_parent = 'Plan_Tier_Voice' then 1 else 0 end) as service_voice\n        ,sum(case when a.plan_parent = 'Plan_Tier_Other' then 1 else 0 end) as service_other\n        ,sum(case when a.plan_parent = 'Plan_Tier_Voice_Service' then 1 else 0 end) as prepaid_voice_service\n        ,sum(case when a.plan_parent = 'Plan_Tier_Data_Service' then 1 else 0 end) as prepaid_data_service\n        ,sum(case when a.plan_parent = 'Plan_Tier_Alternative_Voice' then 1 else 0 end) as prepaid_alternative\n        ,sum(case when a.plan_parent = 'Plan_Tier_Prepaid_Other' then 1 else 0 end) as prepaid_other\n        ,sum(case when a.plan_parent = 'Plan_Tier_Starter' then 1 else 0 end) as mobile_voice_starter\n        ,sum(case when a.plan_parent = 'Plan_Tier_Basic' then 1 else 0 end) as mobile_voice_basic\n        ,sum(case when a.plan_parent = 'Plan_Tier_Advanced' then 1 else 0 end) as mobile_voice_advanced\n        ,sum(case when a.plan_parent = 'Plan_Tier_Premium' then 1 else 0 end) as mobile_voice_premium\n        ,sum(case when a.plan_parent = 'Plan_Tier_Bundle' then 1 else 0 end) as mobile_voice_bundle\n        ,sum(case when a.plan_parent = 'Plan_Tier_Mobile_Other' then 1 else 0 end) as mobile_other\n        ,sum(case when a.plan_parent = 'Plan_Tier_Extra_Small' then 1 else 0 end) as mobile_data_xs\n        ,sum(case when a.plan_parent = 'Plan_Tier_Small' then 1 else 0 end) as mobile_data_s\n        ,sum(case when a.plan_parent = 'Plan_Tier_Medium' then 1 else 0 end) as mobile_data_m\n        ,sum(case when a.plan_parent = 'Plan_Tier_Large' then 1 else 0 end) as mobile_data_l\n        ,sum(case when a.plan_parent = 'Plan_Tier_Extra_Large' then 1 else 0 end) as mobile_data_xl\n        ,sum(case when a.plan_parent = 'Plan_Tier_Mobile_Data' then 1 else 0 end) as mobile_data_other\nfrom service_data_s1 a\nLEFT join (select customer_id, location_id, customer_type from account_daily  where calendar_date in (Select Max(calendar_date) from account_daily ) ) b\n    on a.customer_id = b.customer_id\nLEFT JOIN service_tech c\non a.location_id = c.location_id\nWHERE coalesce(a.location_id, b.location_id) IS NOT NULL\nGROUP BY coalesce(a.location_id, b.location_id)",
        "connectivity_household_canvas": "select distinct\n    hh.address_id\n    ,consumer_count\n    ,consumer_services\n    ,business_count\n    ,business_services\n    ,\"State\"\n    ,city\n    ,postal_code\n    , latitude\n    , longitude\n    ,area_type\n    ,fixed_service_technology\n    ,concat_ws(',',sd.broadband_service,sd.mobile_service,sd.prepaid_service,sd.mobile_broadband) as service_portfolio\n    ,concat_ws(','\n        ,Case when consumer_count > 0 then 'Consumer' end\n        ,Case when business_count > 0 then 'Business' end\n        ) as customer_segment\n    ,al.location_category as location_classification\n    ,service_starter\n    ,service_basic\n    ,service_advanced\n    ,service_premium\n    ,service_ultra\n    ,service_elite\n    ,service_high_speed\n    ,service_voice\n    ,service_satellite\n    ,service_other\n    ,prepaid_voice_service\n    ,prepaid_data_service\n    ,prepaid_alternative\n    , prepaid_other\n    ,mobile_voice_starter\n    ,mobile_voice_basic\n    ,mobile_voice_advanced\n    ,mobile_voice_premium\n    ,mobile_voice_bundle\n    ,mobile_other\n    ,mobile_data_xs\n    ,mobile_data_s\n    ,mobile_data_m\n    ,mobile_data_l\n    ,mobile_data_xl\n    ,mobile_data_other\n    ,   mobile_voice_starter\n        + mobile_voice_basic\n        + mobile_voice_advanced\n        + mobile_voice_premium\n        + mobile_voice_bundle\n        + mobile_other\n        + mobile_data_xs\n        + mobile_data_s\n        + mobile_data_m\n        + mobile_data_l\n        + mobile_data_xl\n        + mobile_data_other\n        + service_starter\n        + service_basic\n        + service_advanced\n        + service_premium\n        + service_ultra\n        + service_elite\n        + service_high_speed\n        + service_satellite\n        + service_other\n        + service_voice\n        + prepaid_voice_service\n        + prepaid_data_service\n        + prepaid_alternative\n        + prepaid_other\n        as total_services_per_household\n    ,   service_starter\n        + service_basic\n        + service_advanced\n        + service_premium\n        + service_ultra\n        + service_elite\n        + service_high_speed\n        + service_satellite\n        + service_other\n        + service_voice\n        as fixed_services_per_household\n    ,   mobile_voice_starter\n        + mobile_voice_basic\n        + mobile_voice_advanced\n        + mobile_voice_premium\n        + mobile_voice_bundle\n        + mobile_data_xs\n        + mobile_data_s\n        + mobile_data_m\n        + mobile_data_l\n        + mobile_data_xl\n        + mobile_data_other\n        + mobile_other\n        as mobile_services_per_household\n    ,   prepaid_voice_service\n        + prepaid_data_service\n        + prepaid_alternative\n        + prepaid_other\n        as prepaid_services_per_household\n        ,case\n            when new_flag is not null then new_flag\n            when active_flag is not null then active_flag\n            when inactive_flag is not null then inactive_flag\n            when service_starter + service_basic + service_advanced + service_premium + service_ultra + service_elite + service_high_speed + service_satellite + service_other >0 then 'Active_Operator_A'\n            else 'Not_With_Operator_A'\n        end as broadband_status\nfrom household_profile_canvas hh\nleft join broadband_active_status fs on fs.address_id = hh.address_id\nleft join service_data sd on sd.address_id = hh.address_id\nleft join (select distinct address_id, location_category from address_location) al on al.address_id = hh.address_id\nwhere calendar_date = (select max(calendar_date) from household_profile_canvas)",
        "main_query": "select\n    hh.address_id\n    ,consumer_count\n    ,business_count\n    ,consumer_services\n    ,business_services\n    ,\"state\"\n    ,city\n    , postal_code\n    , latitude\n    , longitude\n    ,area_type\n    ,fixed_service_technology\n    , nf.technology_type as network_tech_type\n    ,case\n        when service_status is not null then\n            case\n                when service_status = 'Service_Status_A' then 'Network_Type_A'\n                when service_status = 'Service_Status_B' then 'Network_Type_B_Pending'\n                when service_status = 'Service_Status_C' then 'Network_Type_C_Not_Installed'\n                when service_status = 'Service_Status_D' then 'Network_Type_D_Not_Installed'\n                when technology_type = 'Network_Type_A' and service_status = 'Service_Status_E' then 'Network_Type_E_Pending'\n                when technology_type = 'Network_Type_A' and service_status = 'Service_Status_F' then 'Network_Type_F_Pending'\n                when technology_type = 'Network_Type_A' and service_status = 'Service_Status_G' then 'Network_Type_G_Active'\n                when technology_type = 'Network_Type_B' and service_status = 'Service_Status_H' then 'Network_Type_H_Pending'\n                when technology_type = 'Network_Type_B' and service_status = 'Service_Status_I' then 'Network_Type_I_Pending'\n                when technology_type = 'Network_Type_B' and service_status = 'Service_Status_G' then 'Network_Type_B_Active'\n                when service_status = 'Service_Status_J' then 'Network_Type_J_Active'\n                when service_status = 'Service_Status_K' then 'Network_Type_K_Pending'\n                when service_status = 'Service_Status_L' then 'Network_Type_L_Pending'\n                when service_status = 'Service_Status_M' then 'Network_Type_M_Pending'\n                when service_status = 'Service_Status_N' then 'Network_Type_N_Not_Installed'\n                when service_status = 'Service_Status_O' then 'Network_Type_O_Active'\n                when service_status = 'Service_Status_P' then 'Network_Type_P_Pending'\n                when service_status = 'Service_Status_Q' then 'Network_Type_Q_Pending'\n                when service_status = 'Service_Status_R' then 'Network_Type_R_Not_Serviceable'\n                when service_status = 'Network_Pending' then 'Network_Type_Pending'\n                when service_status = 'Service_Status_S' then 'Network_Type_S_Active'\n                when service_status = 'Service_Status_T' then 'Network_Type_T_Pending'\n                when service_status = 'Service_Status_U' then 'Network_Type_U_Pending'\n                when service_status = 'Service_Status_V' then 'Network_Type_V_Planned'\n                when service_status = 'Service_Status_W' then 'Network_Type_W_Planned'\n                when service_status = 'Service_Status_X' then 'Network_Type_X_Planned'\n                else service_status\n            end\n        else 'Not_In_Network_Footprint'\n    end as service_class\n    ,service_restriction\n    ,service_status\n    ,service_portfolio\n    ,customer_segment\n    ,location_classification\n    ,case when nf.address_id is not null then 'Network_A' else 'Network_Other' end as network_type\n    ,segment_type\n    ,segment_group\n    ,affluence\n    ,household_composition\n    ,household_income\n    ,head_of_household_age\n    ,household_lifestage\n    ,child_young_probability\n    ,child_teen_probability\n    ,service_starter as plan_starter\n    ,service_basic as plan_basic\n    ,service_advanced as plan_advanced\n    ,service_premium as plan_premium\n    ,service_ultra\n    ,service_elite\n    ,service_high_speed\n    ,service_voice\n    ,service_satellite\n    ,service_other\n    ,prepaid_voice_service\n    ,prepaid_data_service\n    ,prepaid_alternative\n    ,prepaid_other\n    ,mobile_voice_starter\n    ,mobile_voice_basic\n    ,mobile_voice_advanced\n    ,mobile_voice_premium\n    ,mobile_voice_bundle\n    ,mobile_other\n    ,mobile_data_xs\n    ,mobile_data_s\n    ,mobile_data_m\n    ,mobile_data_l\n    ,mobile_data_xl\n    ,mobile_data_other\n    ,total_services_per_household\n    ,fixed_services_per_household\n    ,mobile_services_per_household\n    ,prepaid_services_per_household\n    ,broadband_status\nfrom connectivity_household_canvas hh\nleft join (select distinct address_id, service_status, service_restriction, technology_type from broadband_footprint) nf on nf.address_id = hh.address_id\nleft join tbl_household_segmentation m on m.address_id = hh.address_id"
      },
      "data_flow": "broadband_canvas, mobile_canvas, prepaid_canvas -> broadband_active_status, service_data_s1 -> service_tech, service_data -> connectivity_household_canvas -> main_query"
    }
  ]
}
''')


def nodes_to_sql(nodes: dict, data_flow: str) -> str:
    """Reconstruct SQL from nodes dict. main_query is always the final SELECT,
    all others become CTEs in data_flow order."""
    if len(nodes) == 1 and "main_query" in nodes:
        return nodes["main_query"]

    # Build CTE order from data_flow string, or just use dict order minus main_query
    cte_names = [k for k in nodes if k != "main_query"]

    parts = ["WITH " + cte_names[0] + " AS (\n" + nodes[cte_names[0]] + "\n)"]
    for name in cte_names[1:]:
        parts.append("," + name + " AS (\n" + nodes[name] + "\n)")
    parts.append(nodes["main_query"])
    return "\n".join(parts)


def compile_optimized_script(original_sql: str, rewrite_sets: list) -> str:
    """Replace targeted CREATE TABLE statements with rewritten versions.
    Non-targeted statements (views, drops, selects) are kept as-is."""

    # Build lookup: target_name -> rewritten inner SQL
    rewrites = {}
    for rs in rewrite_sets:
        target = rs["target"].lower()
        inner_sql = nodes_to_sql(rs["nodes"], rs.get("data_flow", ""))
        rewrites[target] = inner_sql

    lines = original_sql.split('\n')
    result_lines = []
    i = 0
    while i < len(lines):
        line = lines[i]
        line_lower = line.lower().strip()

        # Detect CREATE TEMPORARY TABLE <name> AS
        if 'create temporary table' in line_lower and ' as' in line_lower:
            # Extract table name
            # Pattern: CREATE TEMPORARY TABLE <name> AS
            parts = line_lower.split()
            try:
                tbl_idx = parts.index('table') + 1
                tbl_name = parts[tbl_idx].rstrip()
            except (ValueError, IndexError):
                result_lines.append(line)
                i += 1
                continue

            if tbl_name in rewrites:
                # Found a target - replace everything from this CREATE to the next ;
                result_lines.append(f"CREATE TEMPORARY TABLE {tbl_name} AS")
                result_lines.append(rewrites[tbl_name] + ";")
                # Skip original lines until we find the closing semicolon
                while i < len(lines) and not lines[i].rstrip().endswith(';'):
                    i += 1
                i += 1  # skip the line with ;
                continue

        result_lines.append(line)
        i += 1

    return '\n'.join(result_lines)


def run_script(db_path: str, sql: str, label: str) -> dict:
    """Execute a full SQL script, return per-table row counts and total time."""
    con = duckdb.connect(db_path, read_only=False)

    # Split on semicolons, strip comment-only lines from each fragment,
    # then filter out empty fragments
    raw_stmts = sql.split(';')
    stmts = []
    for s in raw_stmts:
        lines = [l for l in s.split('\n') if l.strip() and not l.strip().startswith('--')]
        clean = '\n'.join(lines).strip()
        if clean:
            stmts.append(clean)

    table_rows = {}
    table_times = {}
    t0 = time.perf_counter()
    for stmt in stmts:
        stmt_lower = stmt.lower().strip()
        clean = stmt

        t_stmt = time.perf_counter()
        try:
            con.execute(clean)
        except Exception as e:
            print(f"  [{label}] ERROR on statement: {str(e)[:200]}")
            print(f"  First 200 chars: {clean[:200]}")
            continue
        t_stmt_end = time.perf_counter()

        # If it created a table, count rows and record time
        if 'create temporary table' in stmt_lower:
            parts = stmt_lower.split()
            try:
                tbl_idx = parts.index('table') + 1
                tbl_name = parts[tbl_idx].strip()
                count = con.execute(f"SELECT COUNT(*) FROM {tbl_name}").fetchone()[0]
                table_rows[tbl_name] = count
                table_times[tbl_name] = t_stmt_end - t_stmt
            except Exception:
                pass

    elapsed = time.perf_counter() - t0
    con.close()
    return {"elapsed": elapsed, "rows": table_rows, "times": table_times}


def main():
    with open(ORIGINAL_SQL) as f:
        original_sql = f.read()

    rs_list = REWRITE_SETS["rewrite_sets"]
    optimized_sql = compile_optimized_script(original_sql, rs_list)

    # Save compiled optimized script for inspection
    out_path = ORIGINAL_SQL.replace('.sql', '_optimized.sql')
    with open(out_path, 'w') as f:
        f.write(optimized_sql)
    print(f"Saved optimized script to: {out_path}")

    # ── 3-run validation: warmup + 2 measured ──────────────────────────
    print("\n" + "="*60)
    print("RUNNING 3x VALIDATION (warmup + avg last 2)")
    print("="*60)

    orig_times = []
    opt_times = []
    orig_rows = {}
    opt_rows = {}

    for run in range(3):
        label = "WARMUP" if run == 0 else f"RUN {run}"
        print(f"\n--- {label} ---")

        print(f"  Running ORIGINAL...")
        r_orig = run_script(DB_PATH, original_sql, f"ORIG-{label}")
        print(f"  Original: {r_orig['elapsed']:.4f}s")
        for tbl, cnt in sorted(r_orig['rows'].items()):
            print(f"    {tbl}: {cnt:,} rows")

        print(f"  Running OPTIMIZED...")
        r_opt = run_script(DB_PATH, optimized_sql, f"OPT-{label}")
        print(f"  Optimized: {r_opt['elapsed']:.4f}s")
        for tbl, cnt in sorted(r_opt['rows'].items()):
            print(f"    {tbl}: {cnt:,} rows")

        if run > 0:
            orig_times.append(r_orig['elapsed'])
            opt_times.append(r_opt['elapsed'])
            for tbl in r_orig.get('times', {}):
                orig_table_times.setdefault(tbl, []).append(r_orig['times'][tbl])
            for tbl in r_opt.get('times', {}):
                opt_table_times.setdefault(tbl, []).append(r_opt['times'][tbl])
        orig_rows = r_orig['rows']
        opt_rows = r_opt['rows']

    # ── Results ──────────────────────────────────────────────────────
    avg_orig = sum(orig_times) / len(orig_times)
    avg_opt = sum(opt_times) / len(opt_times)
    speedup = avg_orig / avg_opt if avg_opt > 0 else float('inf')

    print("\n" + "="*60)
    print("RESULTS")
    print("="*60)
    print(f"Original avg:  {avg_orig:.4f}s  (runs: {[f'{t:.4f}' for t in orig_times]})")
    print(f"Optimized avg: {avg_opt:.4f}s  (runs: {[f'{t:.4f}' for t in opt_times]})")
    print(f"Speedup:       {speedup:.2f}x")

    # Row count comparison
    print("\nRow count comparison:")
    all_tables = sorted(set(list(orig_rows.keys()) + list(opt_rows.keys())))
    all_match = True
    for tbl in all_tables:
        o = orig_rows.get(tbl, 'MISSING')
        n = opt_rows.get(tbl, 'MISSING')
        match = "OK" if o == n else "MISMATCH"
        if o != n:
            all_match = False
        print(f"  {tbl}: orig={o:,} opt={n:,} [{match}]" if isinstance(o, int) and isinstance(n, int)
              else f"  {tbl}: orig={o} opt={n} [{match}]")

    print(f"\nCorrectness: {'PASS' if all_match else 'FAIL'}")


if __name__ == "__main__":
    main()
