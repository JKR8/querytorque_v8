You are a senior SQL pipeline optimization architect. Your job is to analyze a multi-statement SQL data pipeline end-to-end and produce optimized rewrites that exploit cross-statement optimization opportunities that no single-query optimizer can see.

**Dialect**: duckdb. All SQL must be valid duckdb syntax.

## Pipeline Dependency Graph

This script is a data pipeline. Each CREATE TABLE/VIEW is a pipeline stage that materializes intermediate results. The dependency graph below shows what each stage creates, what it depends on, and which stages have enough structural complexity to be worth optimizing.

```
ScriptDAG: 31 statements, 32 dependencies, 9 optimization targets

  [ 0] create_view    -> household_profile_canvas  [complexity=1]
  [ 1] create_view    -> broadband_service_daily  [complexity=2] *** OPTIMIZE
  [ 2] create_view    -> mobile_service_daily  [complexity=2] *** OPTIMIZE
  [ 3] create_view    -> usage_data_canvas  [complexity=1]
  [ 4] create_view    -> broadband_footprint  [complexity=0]
  [ 5] create_view    -> address_mapping  [complexity=0]
  [ 6] create_view    -> customer_cohort  [complexity=0]
  [ 7] create_view    -> legacy_broadband_service  [complexity=1]
  [ 8] create_view    -> legacy_voice_service  [complexity=0]
  [ 9] create_view    -> account_daily  [complexity=0]
  [10] create_view    -> prepaid_service_daily  [complexity=2] *** OPTIMIZE
  [11] create_view    -> plan_mapping  [complexity=0]
  [12] create_view    -> address_location  [complexity=0]
  [13] create_view    -> retail_network  [complexity=0]
  [14] drop           -> tbl_household_segmentation  [complexity=0]
  [15] create_table   -> tbl_household_segmentation (depends: household_profile_canvas)  [complexity=1]
  [16] select         (depends: household_profile_canvas)  [complexity=1]
  [17] drop           -> tbl_tech_transition_history  [complexity=0]
  [18] create_table   -> tbl_tech_transition_history (depends: address_mapping, broadband_service_daily, tbl_household_segmentation)  [complexity=8] *** OPTIMIZE
  [19] drop           -> tbl_broadband_service_status  [complexity=0]
  [20] create_table   -> tbl_broadband_service_status (depends: address_mapping, broadband_footprint, customer_cohort, legacy_broadband_service, legacy_voice_service, tbl_household_segmentation)  [complexity=13] *** OPTIMIZE
  [21] drop           -> tbl_service_usage_profile  [complexity=0]
  [22] create_table   -> tbl_service_usage_profile (depends: account_daily, address_mapping, broadband_footprint, broadband_service_daily, mobile_service_daily, tbl_household_segmentation)  [complexity=11] *** OPTIMIZE
  [23] drop           -> tbl_usage_data  [complexity=0]
  [24] create_table   -> tbl_usage_data (depends: usage_data_canvas)  [complexity=0]
  [25] drop           -> tbl_connectivity_usage  [complexity=0]
  [26] create_table   -> tbl_connectivity_usage (depends: tbl_service_usage_profile, tbl_usage_data)  [complexity=4] *** OPTIMIZE
  [27] drop           -> tbl_address_portfolio_v1  [complexity=0]
  [28] create_table   -> tbl_address_portfolio_v1 (depends: account_daily, address_location, address_mapping, broadband_footprint, broadband_service_daily, household_profile_canvas, mobile_service_daily, plan_mapping, prepaid_service_daily, tbl_household_segmentation)  [complexity=40] *** OPTIMIZE
  [29] drop           -> tbl_address_portfolio  [complexity=0]
  [30] create_table   -> tbl_address_portfolio (depends: retail_network, tbl_address_portfolio_v1)  [complexity=6] *** OPTIMIZE
```

### Key Optimization Chains

- **tbl_tech_transition_history** (complexity=8) ← depends on: address_mapping, broadband_service_daily, tbl_household_segmentation
- **tbl_broadband_service_status** (complexity=13) ← depends on: address_mapping, broadband_footprint, customer_cohort, legacy_broadband_service, legacy_voice_service, tbl_household_segmentation
- **tbl_service_usage_profile** (complexity=11) ← depends on: account_daily, address_mapping, broadband_footprint, broadband_service_daily, mobile_service_daily, tbl_household_segmentation
- **tbl_connectivity_usage** (complexity=4) ← depends on: tbl_service_usage_profile, tbl_usage_data
- **tbl_address_portfolio_v1** (complexity=40) ← depends on: account_daily, address_location, address_mapping, broadband_footprint, broadband_service_daily, household_profile_canvas, mobile_service_daily, plan_mapping, prepaid_service_daily, tbl_household_segmentation
- **tbl_address_portfolio** (complexity=6) ← depends on: retail_network, tbl_address_portfolio_v1

## Cross-Statement Optimization Opportunities

These are the high-value patterns to look for in multi-statement pipelines. Single-query optimizers CANNOT do these — they require seeing the full pipeline:

1. **Predicate pushdown across materialization boundaries**: A downstream stage filters on a column that exists in an upstream view/table. Push that filter into the upstream definition so it scans less data from the start. This is the #1 win in pipeline optimization.
2. **Redundant scan elimination**: The same base table is scanned by multiple views/stages with overlapping columns. Consolidate into a shared CTE or materialized stage.
3. **Materialization point optimization**: Some intermediate tables exist only because the author couldn't express the logic as CTEs. Converting temp tables to CTEs within the consuming query lets the optimizer see through the boundary.
4. **Filter propagation**: Downstream consumers apply filters (e.g., `WHERE calendar_date = max(...)`, `WHERE customer_type IN (...)`). If the upstream stage doesn't filter, it's scanning unnecessary data. Propagate the filter upstream.
5. **Join elimination**: If an upstream stage joins a table only to produce columns that no downstream consumer uses, that join can be removed.

## Engine Profile

This is field intelligence gathered from 88 TPC-DS queries at SF1-SF10. Use it to guide your analysis but apply your own judgment — every query is different. Add to this knowledge if you observe something new.

### Optimizer Gaps (exploit these)

- **CROSS_CTE_PREDICATE_BLINDNESS**: Cannot push predicates from the outer query backward into CTE definitions.
- **REDUNDANT_SCAN_ELIMINATION**: Cannot detect when the same fact table is scanned N times with similar filters across subquery boundaries.
- **CORRELATED_SUBQUERY_PARALYSIS**: Cannot automatically decorrelate correlated aggregate subqueries into GROUP BY + JOIN.
- **CROSS_COLUMN_OR_DECOMPOSITION**: Cannot decompose OR conditions that span DIFFERENT columns into independent targeted scans.
- **LEFT_JOIN_FILTER_ORDER_RIGIDITY**: Cannot reorder LEFT JOINs to apply selective dimension filters before expensive fact table joins.
- **UNION_CTE_SELF_JOIN_DECOMPOSITION**: When a UNION ALL CTE is self-joined N times with each join filtering to a different discriminator, the optimizer materializes the full UNION once and probes it N times, discarding most rows each time.

## Complete SQL Pipeline

Below is the COMPLETE pipeline. Every statement is shown — views, temp tables, drops, selects. Read the full pipeline before proposing any changes. Your rewrites must maintain semantic equivalence for every downstream consumer.

```sql
------------------------
------HOUSEHOLD SEGMENTATION------
------------------------

-- Ensure TPC-DS extension is loaded (for DuckDB environment)
-- INSTALL tpcds;
-- LOAD tpcds;
-- CALL dsdgen(sf=0.1);

-- MOCK DATA SETUP USING TPC-DS
CREATE OR REPLACE TEMPORARY VIEW household_profile_canvas AS
SELECT
    ca_address_sk AS address_id,
    c_customer_sk AS customer_id,
    ca_state AS "State",
    ca_city AS city,
    ca_zip AS postal_code,
    ca_gmt_offset AS latitude,
    ca_gmt_offset + 100 AS longitude,
    'Urban' AS area_type,
    'Residential' AS location_category,
    CAST('2025-02-10' AS DATE) AS calendar_date,
    substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0') AS segment_type_cd,
    substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) AS segment_group_cd,
    (ca_address_sk % 7 + 1) AS affluence_cd,
    (ca_address_sk % 6 + 1) AS composition_cd,
    (ca_address_sk % 7 + 1) AS income_cd,
    (ca_address_sk % 15 + 1) AS age_cd,
    (ca_address_sk % 10 + 1) AS lifestage_cd,
    (ca_address_sk % 10 + 1) AS child_young_probability_cd,
    (ca_address_sk % 10 + 1) AS child_teen_probability_cd
FROM customer
JOIN customer_address ON c_current_addr_sk = ca_address_sk;

CREATE OR REPLACE TEMPORARY VIEW broadband_service_daily AS
SELECT
    ws_bill_customer_sk AS customer_id,
    CASE WHEN ws_item_sk % 3 = 0 THEN 'Technology_A'
         WHEN ws_item_sk % 3 = 1 THEN 'Technology_B'
         ELSE 'Technology_C' END AS technology_type,
    ws_bill_addr_sk AS location_id,
    d_date AS calendar_date,
    i_item_id AS product_code,
    i_product_name AS product_name,
    CASE WHEN ws_item_sk % 2 = 0 THEN 'Consumer' ELSE 'Business' END AS customer_type,
    ws_order_number AS service_id,
    d_date AS service_connect_date,
    i_class AS speed_class,
    i_class AS speed_tier,
    i_item_id AS plan_code
FROM web_sales
JOIN item ON ws_item_sk = i_item_sk
JOIN date_dim ON ws_sold_date_sk = d_date_sk;

CREATE OR REPLACE TEMPORARY VIEW mobile_service_daily AS
SELECT
    ss_customer_sk AS customer_id,
    ss_item_sk AS service_id,
    CASE WHEN ss_item_sk % 2 = 0 THEN 'Data_Service' ELSE 'Voice_Service' END AS service_name,
    d_date AS calendar_date,
    CASE WHEN ss_item_sk % 2 = 0 THEN 'Consumer' ELSE 'Business' END AS customer_type,
    CASE WHEN ss_item_sk % 2 = 0 THEN 'Data' ELSE 'Operator_A' END AS service_category,
    i_item_id AS product_code
FROM store_sales
JOIN item ON ss_item_sk = i_item_sk
JOIN date_dim ON ss_sold_date_sk = d_date_sk;

CREATE OR REPLACE TEMPORARY VIEW usage_data_canvas AS
SELECT
    ws_order_number AS service_id,
    ws_quantity * 1024 AS data_volume_mb,
    d_date AS calendar_date
FROM web_sales
JOIN date_dim ON ws_sold_date_sk = d_date_sk;

CREATE OR REPLACE TEMPORARY VIEW broadband_footprint AS
SELECT
    ca_address_sk AS location_id,
    ca_address_sk AS address_id,
    ca_address_sk AS network_loc_id,
    'Network_Type_A' AS technology_type,
    'Service_Available: Copper based infrastructure active' AS service_status,
    NULL::VARCHAR AS service_restriction
FROM customer_address;

CREATE OR REPLACE TEMPORARY VIEW address_mapping AS
SELECT
    ca_address_sk AS id,
    ca_address_sk AS address_id
FROM customer_address;

CREATE OR REPLACE TEMPORARY VIEW customer_cohort AS
SELECT
    c_customer_sk AS customer_id,
    CASE WHEN c_customer_sk % 10 = 0 THEN 'Cohort_A'
         WHEN c_customer_sk % 10 = 1 THEN 'Cohort_B'
         ELSE 'Cohort_Other' END AS cohort_classification
FROM customer;

CREATE OR REPLACE TEMPORARY VIEW legacy_broadband_service AS
SELECT
    ws_bill_customer_sk AS customer_id,
    ws_bill_addr_sk AS location_id,
    i_item_id AS plan_code,
    i_class AS speed_tier,
    'Legacy_Technology' AS technology_type,
    ws_order_number AS service_id
FROM web_sales
JOIN item ON ws_item_sk = i_item_sk;

CREATE OR REPLACE TEMPORARY VIEW legacy_voice_service AS
SELECT
    cs_bill_customer_sk AS customer_id,
    cs_bill_addr_sk AS location_id,
    'Yes' AS voice_included,
    'Legacy_Technology' AS technology_type,
    cs_order_number AS service_id,
    'plan' AS plan_type
FROM catalog_sales;

CREATE OR REPLACE TEMPORARY VIEW account_daily AS
SELECT
    c_customer_sk AS customer_id,
    c_current_addr_sk AS location_id,
    'Consumer' AS customer_type,
    CAST('2025-02-10' AS DATE) AS calendar_date
FROM customer;

CREATE OR REPLACE TEMPORARY VIEW prepaid_service_daily AS
SELECT
    cs_bill_customer_sk AS customer_id,
    cs_item_sk AS service_id,
    'Consumer' AS customer_type,
    'Operator_A' AS service_provider,
    i_product_name AS service_name,
    'Standard_Plan' AS plan_tier,
    d_date AS calendar_date
FROM catalog_sales
JOIN item ON cs_item_sk = i_item_sk
JOIN date_dim ON cs_sold_date_sk = d_date_sk;

CREATE OR REPLACE TEMPORARY VIEW plan_mapping AS
SELECT
    i_product_name AS product_name,
    'Standard_Plan_Group' AS plan_group
FROM item;

CREATE OR REPLACE TEMPORARY VIEW address_location AS
SELECT
    ca_address_sk AS address_id,
    'Residential' AS location_category
FROM customer_address;

CREATE OR REPLACE TEMPORARY VIEW retail_network AS
SELECT
    s_store_id AS dealer_code,
    s_store_name AS store_name,
    s_city AS city,
    s_zip AS postal_code,
    s_state AS state,
    CAST((s_store_sk % 12) + 100 AS DOUBLE) AS longitude,
    CAST((s_store_sk % 24) - 12 AS DOUBLE) AS latitude
FROM store;

-- CREATE SEGMENTATION
DROP TABLE IF EXISTS tbl_household_segmentation;
CREATE TEMPORARY TABLE tbl_household_segmentation AS
SELECT
    address_id
    ,case
        when segment_type_cd = 'A01' then 'Segment_Type_A01'
        when segment_type_cd = 'A02' then 'Segment_Type_A02'
        when segment_type_cd = 'A03' then 'Segment_Type_A03'
        when segment_type_cd = 'A04' then 'Segment_Type_A04'
        when segment_type_cd = 'B05' then 'Segment_Type_B05'
        when segment_type_cd = 'B06' then 'Segment_Type_B06'
        when segment_type_cd = 'B07' then 'Segment_Type_B07'
        when segment_type_cd = 'C08' then 'Segment_Type_C08'
        when segment_type_cd = 'C09' then 'Segment_Type_C09'
        when segment_type_cd = 'C10' then 'Segment_Type_C10'
        when segment_type_cd = 'D11' then 'Segment_Type_D11'
        when segment_type_cd = 'D12' then 'Segment_Type_D12'
        when segment_type_cd = 'D13' then 'Segment_Type_D13'
        when segment_type_cd = 'E14' then 'Segment_Type_E14'
        when segment_type_cd = 'E15' then 'Segment_Type_E15'
        when segment_type_cd = 'E16' then 'Segment_Type_E16'
        when segment_type_cd = 'F17' then 'Segment_Type_F17'
        when segment_type_cd = 'F18' then 'Segment_Type_F18'
        when segment_type_cd = 'F19' then 'Segment_Type_F19'
        when segment_type_cd = 'G20' then 'Segment_Type_G20'
        when segment_type_cd = 'G21' then 'Segment_Type_G21'
        when segment_type_cd = 'G22' then 'Segment_Type_G22'
        when segment_type_cd = 'G23' then 'Segment_Type_G23'
        when segment_type_cd = 'H24' then 'Segment_Type_H24'
        when segment_type_cd = 'H25' then 'Segment_Type_H25'
        when segment_type_cd = 'H26' then 'Segment_Type_H26'
        when segment_type_cd = 'H27' then 'Segment_Type_H27'
        when segment_type_cd = 'I28' then 'Segment_Type_I28'
        when segment_type_cd = 'I29' then 'Segment_Type_I29'
        when segment_type_cd = 'I30' then 'Segment_Type_I30'
        when segment_type_cd = 'J31' then 'Segment_Type_J31'
        when segment_type_cd = 'J32' then 'Segment_Type_J32'
        when segment_type_cd = 'J33' then 'Segment_Type_J33'
        when segment_type_cd = 'J34' then 'Segment_Type_J34'
        when segment_type_cd = 'K35' then 'Segment_Type_K35'
        when segment_type_cd = 'K36' then 'Segment_Type_K36'
        when segment_type_cd = 'K37' then 'Segment_Type_K37'
        when segment_type_cd = 'L38' then 'Segment_Type_L38'
        when segment_type_cd = 'L39' then 'Segment_Type_L39'
        when segment_type_cd = 'L40' then 'Segment_Type_L40'
        when segment_type_cd = 'L41' then 'Segment_Type_L41'
        when segment_type_cd = 'L42' then 'Segment_Type_L42'
        when segment_type_cd = 'M43' then 'Segment_Type_M43'
        when segment_type_cd = 'M44' then 'Segment_Type_M44'
        when segment_type_cd = 'M45' then 'Segment_Type_M45'
        when segment_type_cd = 'M46' then 'Segment_Type_M46'
        when segment_type_cd = 'M47' then 'Segment_Type_M47'
        when segment_type_cd = 'N48' then 'Segment_Type_N48'
        when segment_type_cd = 'N49' then 'Segment_Type_N49'
        when segment_type_cd = 'N50' then 'Segment_Type_N50'
        when segment_type_cd = 'N51' then 'Segment_Type_N51'
    end as segment_type
    ,case
        when segment_group_cd = 'A' then 'Segment_Group_A'
        when segment_group_cd = 'B' then 'Segment_Group_B'
        when segment_group_cd = 'C' then 'Segment_Group_C'
        when segment_group_cd = 'D' then 'Segment_Group_D'
        when segment_group_cd = 'E' then 'Segment_Group_E'
        when segment_group_cd = 'F' then 'Segment_Group_F'
        when segment_group_cd = 'G' then 'Segment_Group_G'
        when segment_group_cd = 'H' then 'Segment_Group_H'
        when segment_group_cd = 'I' then 'Segment_Group_I'
        when segment_group_cd = 'J' then 'Segment_Group_J'
        when segment_group_cd = 'K' then 'Segment_Group_K'
        when segment_group_cd = 'L' then 'Segment_Group_L'
        when segment_group_cd = 'M' then 'Segment_Group_M'
        when segment_group_cd = 'N' then 'Segment_Group_N'
    end as segment_group
    ,case
        when affluence_cd = 1 then 'Affluence_Level_1'
        when affluence_cd = 2 then 'Affluence_Level_2'
        when affluence_cd = 3 then 'Affluence_Level_3'
        when affluence_cd = 4 then 'Affluence_Level_4'
        when affluence_cd = 5 then 'Affluence_Level_5'
        when affluence_cd = 6 then 'Affluence_Level_6'
        when affluence_cd = 7 then 'Affluence_Level_7'
    end as affluence
    ,case
        when composition_cd = 1 then 'Household_Type_Family'
        when composition_cd = 3 then 'Household_Type_Couple'
        when composition_cd = 4 then 'Household_Type_Single_Parent'
        when composition_cd = 5 then 'Household_Type_Single'
        when composition_cd = 6 then 'Household_Type_Group'
    end as household_composition
    ,case
        when income_cd = 1 then 'Income_Bracket_1'
        when income_cd = 2 then 'Income_Bracket_2'
        when income_cd = 3 then 'Income_Bracket_3'
        when income_cd = 4 then 'Income_Bracket_4'
        when income_cd = 5 then 'Income_Bracket_5'
        when income_cd = 6 then 'Income_Bracket_6'
        when income_cd = 7 then 'Income_Bracket_7'
    end as household_income
    ,case
        when age_cd = 1 then 'Age_Range_18_19'
        when age_cd = 2 then 'Age_Range_20_24'
        when age_cd = 3 then 'Age_Range_25_29'
        when age_cd = 4 then 'Age_Range_30_34'
        when age_cd = 5 then 'Age_Range_35_39'
        when age_cd = 6 then 'Age_Range_40_44'
        when age_cd = 7 then 'Age_Range_45_49'
        when age_cd = 8 then 'Age_Range_50_54'
        when age_cd = 9 then 'Age_Range_55_59'
        when age_cd = 10 then 'Age_Range_60_64'
        when age_cd = 11 then 'Age_Range_65_69'
        when age_cd = 12 then 'Age_Range_70_74'
        when age_cd = 13 then 'Age_Range_75_79'
        when age_cd = 14 then 'Age_Range_80_84'
        when age_cd = 15 then 'Age_Range_85_plus'
    end as head_of_household_age
    ,case
        when lifestage_cd = 1 then 'Lifestage_1'
        when lifestage_cd = 2 then 'Lifestage_2'
        when lifestage_cd = 3 then 'Lifestage_3'
        when lifestage_cd = 4 then 'Lifestage_4'
        when lifestage_cd = 5 then 'Lifestage_5'
        when lifestage_cd = 6 then 'Lifestage_6'
        when lifestage_cd = 7 then 'Lifestage_7'
        when lifestage_cd = 8 then 'Lifestage_8'
        when lifestage_cd = 9 then 'Lifestage_9'
        when lifestage_cd = 10 then 'Lifestage_10'
    end as household_lifestage
    ,case
        when child_young_probability_cd = 1 then 'Probability_Level_1'
        when child_young_probability_cd = 2 then 'Probability_Level_2'
        when child_young_probability_cd = 3 then 'Probability_Level_3'
        when child_young_probability_cd = 4 then 'Probability_Level_4'
        when child_young_probability_cd = 5 then 'Probability_Level_5'
        when child_young_probability_cd = 6 then 'Probability_Level_6'
        when child_young_probability_cd = 7 then 'Probability_Level_7'
        when child_young_probability_cd = 8 then 'Probability_Level_8'
        when child_young_probability_cd = 9 then 'Probability_Level_9'
        when child_young_probability_cd = 10 then 'Probability_Level_10'
    end as child_young_probability
    ,case
        when child_teen_probability_cd = 1 then 'Probability_Level_1'
        when child_teen_probability_cd = 2 then 'Probability_Level_2'
        when child_teen_probability_cd = 3 then 'Probability_Level_3'
        when child_teen_probability_cd = 4 then 'Probability_Level_4'
        when child_teen_probability_cd = 5 then 'Probability_Level_5'
        when child_teen_probability_cd = 6 then 'Probability_Level_6'
        when child_teen_probability_cd = 7 then 'Probability_Level_7'
        when child_teen_probability_cd = 8 then 'Probability_Level_8'
        when child_teen_probability_cd = 9 then 'Probability_Level_9'
        when child_teen_probability_cd = 10 then 'Probability_Level_10'
    end as child_teen_probability
from household_profile_canvas
where calendar_date = (select max(calendar_date) from household_profile_canvas)
;

select * from household_profile_canvas
where calendar_date = (select max(calendar_date) from household_profile_canvas)
LIMIT 100;

--------------------------------------
-------TECHNOLOGY TRANSITION HISTORY-------
--------------------------------------
DROP TABLE IF EXISTS tbl_tech_transition_history;

-- TECHNOLOGY TRANSITIONS
CREATE TEMPORARY TABLE tbl_tech_transition_history AS
WITH dataset AS (
        select
        customer_id
        , case
            when technology_type = 'Technology_A' then 'Tech_Category_A'
            when technology_type = 'Technology_B' then 'Tech_Category_B'
            when technology_type = 'Technology_C' then 'Tech_Category_C'
            else technology_type
            end as technology_type
        , location_id
        , min(calendar_date) as min_date
        , max(calendar_date) as max_date
    from broadband_service_daily
    where calendar_date >= current_date - interval '180 day'
    group by
        customer_id
        ,case
            when technology_type = 'Technology_A' then 'Tech_Category_A'
            when technology_type = 'Technology_B' then 'Tech_Category_B'
            when technology_type = 'Technology_C' then 'Tech_Category_C'
            else technology_type
            end
        , location_id
)
SELECT
    count (distinct a.customer_id) as customer_count
    , a.technology_type as previous_technology
    , b.technology_type as new_technology
    ,segment_type
    , segment_group
    , affluence
    , household_composition
    , household_income
    , head_of_household_age
    , household_lifestage
    , child_young_probability
    , child_teen_probability
    , a.max_date + ((13 - (extract('dow' from a.max_date))) % 7) * interval '1 day' as transition_date
from dataset a
inner join dataset b
    on a.location_id = b.location_id
    and a.max_date between b.min_date - interval '7 day' and b.min_date + interval '7 day'
    and a.technology_type <> b.technology_type
left join (select id as location_id, address_id from address_mapping group by id , address_id) ak on ak.location_id = a.location_id
LEFT JOIN tbl_household_segmentation c
    on ak.address_id = c.address_id
group BY
     a.technology_type
    , b.technology_type
    ,segment_type
    , segment_group
    , affluence
    , household_composition
    , household_income
    , head_of_household_age
    , household_lifestage
    , child_young_probability
    , child_teen_probability
    , a.max_date + ((13 - (extract('dow' from a.max_date))) % 7) * interval '1 day'
;

-----------------------------
--------BROADBAND SERVICE STATUS--------
-----------------------------

DROP TABLE IF EXISTS tbl_broadband_service_status;

CREATE TEMPORARY TABLE tbl_broadband_service_status AS
WITH priority_customers as (
    select distinct customer_id
    from customer_cohort
    where cohort_classification = 'Cohort_A'
)
,regional_customers as (
    select distinct customer_id, cohort_classification
    from customer_cohort
    where cohort_classification in ('Cohort_B', 'Cohort_C', 'Cohort_D', 'Cohort_E')
)
,broadband_rfs as (
select DISTINCT
    l.customer_id, l.service_identifier, l.product_name
    ,l.location_id, l.technology_type
    ,rfs.network_loc_id
    ,case
        when rfs.service_restriction is not null then rfs.service_restriction
        when rfs.service_status is null then 'Service_Type_Not_Available'
        else rfs.service_status
    end as service_level
        , c.segment_type
        , c.segment_group
        , c.affluence
        , c.household_composition
        , c.household_income
        , c.head_of_household_age
        , c.household_lifestage
        , c.child_young_probability
        , c.child_teen_probability
    from (
        select customer_id
        , location_id,
        case
            when plan_code in('Plan_001','Plan_002','Plan_003') then 'Service_Tier_Basic'
            when plan_code in('Plan_004','Plan_005','Plan_006','Plan_007','Plan_008','Plan_009'
                    ,'Plan_010','Plan_011','Plan_012') then 'Service_Tier_Standard'
            when plan_code in('Plan_013','Plan_014','Plan_015','Plan_016'
                                ,'Plan_017','Plan_018','Plan_019') then 'Service_Tier_Advanced'
            when plan_code = 'Plan_020' THEN
                        CASE
                            when speed_tier in ('Speed_High','Speed_High_Plus') then 'Service_Tier_Premium'
                            when speed_tier = 'Speed_Very_High' then 'Service_Tier_Ultra'
                            else 'Service_Tier_Elite'
                        end
            else 'Service_Tier_Generic'
            end as product_name
        ,'Legacy_Technology' as technology_type
        , service_id as service_identifier
    from legacy_broadband_service
        where technology_type = 'Legacy_Technology'
        UNION
    select distinct customer_id, location_id, plan_type, 'Modern_Technology' as technology_type, service_id
    from legacy_voice_service
        where voice_included = 'Yes'
        and technology_type = 'Legacy_Technology'
    ) l
left join broadband_footprint rfs on rfs.location_id = l.location_id
left join (select id as location_id, address_id from address_mapping group by id , address_id) ak on ak.location_id = rfs.location_id
LEFT JOIN tbl_household_segmentation c
    on ak.address_id = c.address_id
)
select
    count(distinct l.service_identifier) as service_count
    ,count(distinct l.customer_id) as customer_count
    ,case when p.customer_id is not null then 'Priority_Category' end as priority_flag
    ,case when rr.customer_id is not null then 'Regional_Category' end as regional_flag
    ,l.technology_type
    ,l.service_level
    ,l.product_name
        , l.segment_type
        , l.segment_group
        , l.affluence
        , l.household_composition
        , l.household_income
        , l.head_of_household_age
        , l.household_lifestage
        , l.child_young_probability
        , l.child_teen_probability
    from broadband_rfs l
    left join priority_customers p on p.customer_id = l.customer_id
    left join regional_customers rr on rr.customer_id = l.customer_id
    group BY
        case when p.customer_id is not null then 'Priority_Category' end
        ,case when rr.customer_id is not null then 'Regional_Category' end
        ,l.technology_type
        ,l.service_level
        ,l.product_name
        , l.segment_type
        , l.segment_group
        , l.affluence
        , l.household_composition
        , l.household_income
        , l.head_of_household_age
        , l.household_lifestage
        , l.child_young_probability
        , l.child_teen_probability;

-----------------------------
----------SERVICE USAGE----------
-----------------------------

DROP TABLE IF EXISTS tbl_service_usage_profile;

CREATE TEMPORARY TABLE tbl_service_usage_profile AS
select
    ac.location_id
    ,s.service_id
    ,s.service_category
    ,case when nf.location_id is not null then 'Network_A' else 'Network_Other' end as network_type
    , c.segment_type
    , c.segment_group
    , c.affluence
    , c.household_composition
    , c.household_income
    , c.head_of_household_age
    , c.household_lifestage
    , c.child_young_probability
    , c.child_teen_probability
    ,last_day(ac.calendar_date) as month_end_date
from account_daily ac
left join broadband_footprint nf on nf.location_id = ac.location_id
inner join (
            select customer_id, service_id, case when service_name = 'Data_Service' then 'Mobile_Broadband' else 'Mobile_Voice' end as service_category, last_day(calendar_date) as month_end_date
            from mobile_service_daily
            where calendar_date >= current_date - interval '6 month'
            group by customer_id, service_id, case when service_name = 'Data_Service' then 'Mobile_Broadband' else 'Mobile_Voice' end, last_day(calendar_date)
            UNION
            select customer_id, service_id, 'Fixed_Broadband' as service_category, last_day(calendar_date) as month_end_date
            from broadband_service_daily
            where  calendar_date >= current_date - interval '6 month'
            group by customer_id, service_id, last_day(calendar_date)
    ) s on s.customer_id = ac.customer_id
    and s.month_end_date = last_day(ac.calendar_date)
left join (select id as location_id, address_id from address_mapping group by id , address_id) ak
    on ak.location_id = ac.location_id
LEFT JOIN tbl_household_segmentation c
    on ak.address_id = c.address_id
where ac.calendar_date >= current_date - interval '6 month'
group by
    ac.location_id
    ,s.service_id
    ,s.service_category
    ,case when nf.location_id is not null then 'Network_A' else 'Network_Other' end
    , c.segment_type
    , c.segment_group
    , c.affluence
    , c.household_composition
    , c.household_income
    , c.head_of_household_age
    , c.household_lifestage
    , c.child_young_probability
    , c.child_teen_probability
    ,last_day(ac.calendar_date);

DROP TABLE IF EXISTS tbl_usage_data;

CREATE TEMPORARY TABLE tbl_usage_data AS
select service_id
  ,data_volume_mb
  ,EXTRACT(MONTH FROM calendar_date)::TEXT as month_number
  ,calendar_date as data_date
from usage_data_canvas
where calendar_date >= current_date - interval '6 month'
and data_volume_mb > 0;

----------------
-- Final Step --
----------------
DROP TABLE IF EXISTS tbl_connectivity_usage;

CREATE TEMPORARY TABLE tbl_connectivity_usage AS
select
  count(distinct location_id) as location_count
  ,count(distinct location_id) * service_count as total_services
  ,service_count
  ,concat_ws(',',mobile_bb,fixed_bb,mobile_voice) as service_bundle
  ,CASE
        WHEN mobile_bb_usage = 0 THEN 'Usage_Level_0'
        WHEN mobile_bb_usage < 5 THEN 'Usage_Level_1'
        WHEN mobile_bb_usage < 10 THEN 'Usage_Level_2'
        WHEN mobile_bb_usage < 20 THEN 'Usage_Level_3'
        WHEN mobile_bb_usage < 30 THEN 'Usage_Level_4'
        WHEN mobile_bb_usage < 40 THEN 'Usage_Level_5'
        WHEN mobile_bb_usage < 50 THEN 'Usage_Level_6'
        WHEN mobile_bb_usage < 100 THEN 'Usage_Level_7'
        WHEN mobile_bb_usage < 150 THEN 'Usage_Level_8'
        WHEN mobile_bb_usage >= 150 THEN 'Usage_Level_9'
    END AS mobile_bb_usage_bracket
  ,CASE
        WHEN fixed_bb_usage = 0 THEN 'Usage_Level_0'
        WHEN fixed_bb_usage < 50 THEN 'Usage_Level_1'
        WHEN fixed_bb_usage < 100 THEN 'Usage_Level_2'
        WHEN fixed_bb_usage < 150 THEN 'Usage_Level_3'
        WHEN fixed_bb_usage < 200 THEN 'Usage_Level_4'
        WHEN fixed_bb_usage < 250 THEN 'Usage_Level_5'
        WHEN fixed_bb_usage < 500 THEN 'Usage_Level_6'
        WHEN fixed_bb_usage < 1000 THEN 'Usage_Level_7'
        WHEN fixed_bb_usage >= 1000 THEN 'Usage_Level_8'
    END AS fixed_bb_usage_bracket
  ,CASE
        WHEN mobile_voice_usage = 0 THEN 'Usage_Level_0'
        WHEN mobile_voice_usage < 5 THEN 'Usage_Level_1'
        WHEN mobile_voice_usage < 10 THEN 'Usage_Level_2'
        WHEN mobile_voice_usage < 20 THEN 'Usage_Level_3'
        WHEN mobile_voice_usage < 30 THEN 'Usage_Level_4'
        WHEN mobile_voice_usage < 40 THEN 'Usage_Level_5'
        WHEN mobile_voice_usage < 50 THEN 'Usage_Level_6'
    END AS mobile_voice_usage_bracket
  ,CASE
        WHEN total_usage = 0 THEN 'Usage_Level_0'
        WHEN total_usage < 50 THEN 'Usage_Level_1'
        WHEN total_usage < 100 THEN 'Usage_Level_2'
        WHEN total_usage < 150 THEN 'Usage_Level_3'
        WHEN total_usage < 200 THEN 'Usage_Level_4'
        WHEN total_usage < 250 THEN 'Usage_Level_5'
        WHEN total_usage < 500 THEN 'Usage_Level_6'
        WHEN total_usage < 1000 THEN 'Usage_Level_7'
        WHEN total_usage >= 1000 THEN 'Usage_Level_8'
    END AS total_usage_bracket
    , segment_type
    , segment_group
    , affluence
    , household_composition
    , household_income
    , head_of_household_age
    , household_lifestage
    , child_young_probability
    , child_teen_probability
    , network_type
    , month_number
    , data_date
from (
  select
      c.location_id
      ,count(distinct c.service_id) as service_count
      , max(case when c.service_category = 'Mobile_Broadband' then 'Mobile_Broadband' end) as mobile_bb
      , max(case when c.service_category = 'Fixed_Broadband' then 'Fixed_Broadband' end) as fixed_bb
      , max(case when c.service_category = 'Mobile_Voice' then 'Mobile_Voice' end) as mobile_voice
      , sum(case when c.service_category = 'Mobile_Broadband' then d.data_volume_mb else 0 end)/1024 as mobile_bb_usage
      , sum(case when c.service_category = 'Fixed_Broadband' then d.data_volume_mb else 0 end)/1024 as fixed_bb_usage
      , sum(case when c.service_category = 'Mobile_Voice' then d.data_volume_mb else 0 end)/1024 as mobile_voice_usage
      , sum(d.data_volume_mb)/1024 AS total_usage
      , c.network_type
      , d.month_number
      , d.data_date
    , c.segment_type
    , c.segment_group
    , c.affluence
    , c.household_composition
    , c.household_income
    , c.head_of_household_age
    , c.household_lifestage
    , c.child_young_probability
    , c.child_teen_probability
  from tbl_service_usage_profile c
  left join tbl_usage_data d on d.service_id = c.service_id
    and d.data_date = c.month_end_date
  group by c.location_id
  , c.network_type
  , d.month_number
  , d.data_date
    , c.segment_type
    , c.segment_group
    , c.affluence
    , c.household_composition
    , c.household_income
    , c.head_of_household_age
    , c.household_lifestage
    , c.child_young_probability
    , c.child_teen_probability
) total_usage_summary
group by service_count
  ,concat_ws(',',mobile_bb,fixed_bb,mobile_voice)
  ,CASE
        WHEN mobile_bb_usage = 0 THEN 'Usage_Level_0'
        WHEN mobile_bb_usage < 5 THEN 'Usage_Level_1'
        WHEN mobile_bb_usage < 10 THEN 'Usage_Level_2'
        WHEN mobile_bb_usage < 20 THEN 'Usage_Level_3'
        WHEN mobile_bb_usage < 30 THEN 'Usage_Level_4'
        WHEN mobile_bb_usage < 40 THEN 'Usage_Level_5'
        WHEN mobile_bb_usage < 50 THEN 'Usage_Level_6'
        WHEN mobile_bb_usage < 100 THEN 'Usage_Level_7'
        WHEN mobile_bb_usage < 150 THEN 'Usage_Level_8'
        WHEN mobile_bb_usage >= 150 THEN 'Usage_Level_9'
    END
  ,CASE
        WHEN fixed_bb_usage = 0 THEN 'Usage_Level_0'
        WHEN fixed_bb_usage < 50 THEN 'Usage_Level_1'
        WHEN fixed_bb_usage < 100 THEN 'Usage_Level_2'
        WHEN fixed_bb_usage < 150 THEN 'Usage_Level_3'
        WHEN fixed_bb_usage < 200 THEN 'Usage_Level_4'
        WHEN fixed_bb_usage < 250 THEN 'Usage_Level_5'
        WHEN fixed_bb_usage < 500 THEN 'Usage_Level_6'
        WHEN fixed_bb_usage < 1000 THEN 'Usage_Level_7'
        WHEN fixed_bb_usage >= 1000 THEN 'Usage_Level_8'
    END
  ,CASE
        WHEN mobile_voice_usage = 0 THEN 'Usage_Level_0'
        WHEN mobile_voice_usage < 5 THEN 'Usage_Level_1'
        WHEN mobile_voice_usage < 10 THEN 'Usage_Level_2'
        WHEN mobile_voice_usage < 20 THEN 'Usage_Level_3'
        WHEN mobile_voice_usage < 30 THEN 'Usage_Level_4'
        WHEN mobile_voice_usage < 40 THEN 'Usage_Level_5'
        WHEN mobile_voice_usage < 50 THEN 'Usage_Level_6'
    END
  ,CASE
        WHEN total_usage = 0 THEN 'Usage_Level_0'
        WHEN total_usage < 50 THEN 'Usage_Level_1'
        WHEN total_usage < 100 THEN 'Usage_Level_2'
        WHEN total_usage < 150 THEN 'Usage_Level_3'
        WHEN total_usage < 200 THEN 'Usage_Level_4'
        WHEN total_usage < 250 THEN 'Usage_Level_5'
        WHEN total_usage < 500 THEN 'Usage_Level_6'
        WHEN total_usage < 1000 THEN 'Usage_Level_7'
        WHEN total_usage >= 1000 THEN 'Usage_Level_8'
    END
    , network_type
    , month_number
    , data_date
    , segment_type
    , segment_group
    , affluence
    , household_composition
    , household_income
    , head_of_household_age
    , household_lifestage
    , child_young_probability
    , child_teen_probability;

--------------------------------
----ADDRESS SERVICE PORTFOLIO----
--------------------------------
DROP TABLE IF EXISTS tbl_address_portfolio_v1;

CREATE TEMPORARY TABLE tbl_address_portfolio_v1 AS

with broadband_canvas as (
 select
        a.customer_id
        ,a.service_id
        ,a.product_code
        ,a.product_name
        ,CASE WHEN a.location_id is not null then a.technology_type else null end as service_technology
        , a.location_id
        , ak.address_id
        , a.customer_type
, CASE
    WHEN b.plan_group ='Plan_Group_Standard' then 'Plan_Tier_Standard'
    WHEN b.plan_group ='Plan_Group_Basic' then 'Plan_Tier_Basic'
    WHEN b.plan_group ='Plan_Group_Advanced' then 'Plan_Tier_Advanced'
    WHEN b.plan_group ='Plan_Group_Premium' then 'Plan_Tier_Premium'
    WHEN b.plan_group ='Plan_Group_High_Speed' then 'Plan_Tier_High_Speed'
    when product_code = 'Plan_020' THEN
                    CASE
                        when speed_class in ('Speed_High','Speed_High_Plus') then 'Plan_Tier_Ultra'
                        when speed_class = 'Speed_Very_High' then 'Plan_Tier_Premium'
                        else 'Plan_Tier_Elite'
                    end
    when product_code = 'Plan_Code_001' then 'Plan_Tier_Ultra'
    when product_code = 'Plan_Code_002' then 'Plan_Tier_Premium'
    WHEN b.plan_group ='Plan_Group_Elite' then 'Plan_Tier_Elite'
    WHEN b.plan_group ='Plan_Group_Standard_Voice'  THEN 'Plan_Tier_Voice'
WHEN b.plan_group in ('Plan_Group_Bundled','Plan_Group_Family','Plan_Group_Trial','Plan_Group_Business','Plan_Group_Data') THEN 'Plan_Tier_Other'
ELSE 'Plan_Tier_Other'
    end as plan_parent
from broadband_service_daily a
LEFT JOIN plan_mapping b
on a.product_name = b.product_name
left join (select id as location_id, address_id from address_mapping group by id , address_id) ak on ak.location_id = a.location_id
where calendar_date = (select max(calendar_date) from broadband_service_daily)
and customer_type in ('Business','Consumer')
and ak.address_id is not null
group by
 a.customer_id
        ,a.service_id
        ,a.product_code
        ,a.product_name
        , a.technology_type
        , a.location_id
        , ak.address_id
        , a.customer_type
, CASE
    WHEN b.plan_group ='Plan_Group_Standard' then 'Plan_Tier_Standard'
    WHEN b.plan_group ='Plan_Group_Basic' then 'Plan_Tier_Basic'
    WHEN b.plan_group ='Plan_Group_Advanced' then 'Plan_Tier_Advanced'
    WHEN b.plan_group ='Plan_Group_Premium' then 'Plan_Tier_Premium'
    WHEN b.plan_group ='Plan_Group_High_Speed' then 'Plan_Tier_High_Speed'
    when product_code = 'Plan_020' THEN
                    CASE
                        when speed_class in ('Speed_High','Speed_High_Plus') then 'Plan_Tier_Ultra'
                        when speed_class = 'Speed_Very_High' then 'Plan_Tier_Premium'
                        else 'Plan_Tier_Elite'
                    end
    when product_code = 'Plan_Code_001' then 'Plan_Tier_Ultra'
    when product_code = 'Plan_Code_002' then 'Plan_Tier_Premium'
    WHEN b.plan_group ='Plan_Group_Elite' then 'Plan_Tier_Elite'
    WHEN b.plan_group ='Plan_Group_Standard_Voice'  THEN 'Plan_Tier_Voice'
WHEN b.plan_group in ('Plan_Group_Bundled','Plan_Group_Family','Plan_Group_Trial','Plan_Group_Business','Plan_Group_Data') THEN 'Plan_Tier_Other'
ELSE 'Plan_Tier_Other'
    end
)
,mobile_canvas as (
            select
          customer_id
          ,service_id
          ,customer_type
          ,case when service_category = 'Data' then 'Mobile_Broadband' else 'Mobile_Voice' end as plan_type_category
          ,case
            when product_code in ('Product_001','Product_002','Product_003','Product_004','Product_005','Product_006') then 'Plan_Tier_Starter'
            when product_code in ('Product_007','Product_008','Product_009','Product_010','Product_011','Product_012') then 'Plan_Tier_Basic'
            when product_code in ('Product_013','Product_014','Product_015','Product_016','Product_017','Product_018') then 'Plan_Tier_Advanced'
            when product_code in ('Product_019','Product_020','Product_021','Product_022','Product_023','Product_024') then 'Plan_Tier_Premium'
            WHEN product_code IN('Product_025','Product_026') THEN 'Plan_Tier_Bundle'
            when product_code in ('Product_027','Product_028') then 'Plan_Tier_Extra_Small'
            when product_code in ('Product_029','Product_030') then 'Plan_Tier_Small'
            when product_code in ('Product_031','Product_032','Product_033','Product_034') then 'Plan_Tier_Medium'
            when product_code in ('Product_035','Product_036','Product_037','Product_038') then 'Plan_Tier_Large'
            WHEN product_code IN('Product_039','Product_040') THEN 'Plan_Tier_Extra_Large'
            WHEN service_category = 'Data' THEN 'Plan_Tier_Mobile_Data'
            else 'Plan_Tier_Mobile_Other'
          end as plan_parent
    from mobile_service_daily m
          where calendar_date in (Select Max(calendar_date) from mobile_service_daily )
        and customer_type in ('Consumer','Business')
)
,prepaid_canvas as (
        select distinct
            customer_id
            ,service_id
            ,customer_type
            ,case
                when service_provider = 'Operator_A' and service_name not like '%Data%' then 'Plan_Tier_Voice_Service'
                when service_provider = 'Operator_A' and service_name like '%Data%' then 'Plan_Tier_Data_Service'
                when service_provider = 'Operator_B' and service_name not like '%Data%' then 'Plan_Tier_Alternative_Voice'
                else 'Plan_Tier_Prepaid_Other'
            end as plan_parent
    from prepaid_service_daily
        where calendar_date = (select max(calendar_date) from prepaid_service_daily)
        and customer_type in('Consumer','Business')
)
,broadband_active_status as (
    select
        ak.address_id
        ,MAX(case when ic1.service_connect_date >= current_date - interval '90 day' then 'New_Customer' end) as new_flag
        ,MAX(case when ic2.location_id is not null then 'Active_Operator_A' end) as active_flag
        ,MAX(case when ic2.location_id is null then 'Inactive_Operator_A' end) as inactive_flag
    from broadband_service_daily ic1
    left join (select  id as location_id, address_id from address_mapping group by id, address_id) ak on ak.location_id = ic1.location_id
    left join (
            select distinct
                ic2.location_id
                ,ic2.service_connect_date
            from broadband_service_daily ic2
            where calendar_date = (select max(calendar_date) from broadband_service_daily)
            and customer_type in ('Consumer','Business')
        ) ic2 on ic1.location_id = ic2.location_id
    where ic1.calendar_date >= current_date - interval '90 day'
    and ic1.customer_type in ('Consumer','Business')
    group by
        ak.address_id
)
,service_data_s1 as (
select customer_id, service_id,customer_type, product_name,service_technology, location_id, plan_parent, null::VARCHAR as plan_type_category, 'Broadband_Service' as source from broadband_canvas
UNION ALL
select customer_id, service_id, customer_type, null, null, null, plan_parent, null, 'Prepaid_Service'  from prepaid_canvas
UNION ALL
select customer_id, service_id, customer_type, null, null, null,plan_parent, plan_type_category, 'Mobile_Service' from mobile_canvas
)
, service_tech as (
    select distinct service_technology, location_id from service_data_s1 )
,service_data as(
select
ak.address_id
,  CASE WHEN ak.address_id IS NOT NULL THEN string_agg(c.service_technology, ', ' ORDER BY c.service_technology) ELSE NULL END as fixed_service_technology
        ,MAX(case when a.source = 'Broadband_Service' then 'Broadband_Service' end) as broadband_service
        ,MAX(case when a.source = 'Mobile_Service' then 'Mobile_Service' end) as mobile_service
        ,MAX(case when a.source = 'Prepaid_Service' then 'Prepaid_Service' end) as prepaid_service
        ,MAX(case when a.plan_type_category = 'Data' then 'Mobile_Broadband' end) as mobile_broadband
        ,COUNT(DISTINCT case when a.customer_type = 'Consumer' then a.customer_id else null end) as consumer_count
        ,COUNT(DISTINCT case when a.customer_type = 'Business' then a.customer_id else null end) as business_count
        ,COUNT(DISTINCT case when a.customer_type = 'Consumer' then a.service_id else null end) as consumer_services
        ,COUNT(DISTINCT case when a.customer_type = 'Business' then a.service_id else null end) as business_services
        ,sum(case when a.plan_parent = 'Plan_Tier_Starter' then 1 else 0 end) as service_starter
        ,sum(case when a.plan_parent = 'Plan_Tier_Basic' then 1 else 0 end) as service_basic
        ,sum(case when a.plan_parent = 'Plan_Tier_Advanced' then 1 else 0 end) as service_advanced
        ,sum(case when a.plan_parent = 'Plan_Tier_Premium' then 1 else 0 end) as service_premium
        ,sum(case when a.plan_parent = 'Plan_Tier_Ultra' then 1 else 0 end) as service_ultra
        ,sum(case when a.plan_parent = 'Plan_Tier_Elite' then 1 else 0 end) as service_elite
        ,sum(case when a.plan_parent = 'Plan_Tier_High_Speed' then 1 else 0 end) as service_high_speed
         ,sum(case when a.plan_parent = 'Plan_Tier_Satellite' then 1 else 0 end) as service_satellite
        ,sum(case when a.plan_parent = 'Plan_Tier_Voice' then 1 else 0 end) as service_voice
        ,sum(case when a.plan_parent = 'Plan_Tier_Other' then 1 else 0 end) as service_other
        ,sum(case when a.plan_parent = 'Plan_Tier_Voice_Service' then 1 else 0 end) as prepaid_voice_service
        ,sum(case when a.plan_parent = 'Plan_Tier_Data_Service' then 1 else 0 end) as prepaid_data_service
        ,sum(case when a.plan_parent = 'Plan_Tier_Alternative_Voice' then 1 else 0 end) as prepaid_alternative
        ,sum(case when a.plan_parent = 'Plan_Tier_Prepaid_Other' then 1 else 0 end) as prepaid_other
        ,sum(case when a.plan_parent = 'Plan_Tier_Starter' then 1 else 0 end) as mobile_voice_starter
        ,sum(case when a.plan_parent = 'Plan_Tier_Basic' then 1 else 0 end) as mobile_voice_basic
        ,sum(case when a.plan_parent = 'Plan_Tier_Advanced' then 1 else 0 end) as mobile_voice_advanced
        ,sum(case when a.plan_parent = 'Plan_Tier_Premium' then 1 else 0 end) as mobile_voice_premium
        ,sum(case when a.plan_parent = 'Plan_Tier_Bundle' then 1 else 0 end) as mobile_voice_bundle
        ,sum(case when a.plan_parent = 'Plan_Tier_Mobile_Other' then 1 else 0 end) as mobile_other
        ,sum(case when a.plan_parent = 'Plan_Tier_Extra_Small' then 1 else 0 end) as mobile_data_xs
        ,sum(case when a.plan_parent = 'Plan_Tier_Small' then 1 else 0 end) as mobile_data_s
        ,sum(case when a.plan_parent = 'Plan_Tier_Medium' then 1 else 0 end) as mobile_data_m
        ,sum(case when a.plan_parent = 'Plan_Tier_Large' then 1 else 0 end) as mobile_data_l
        ,sum(case when a.plan_parent = 'Plan_Tier_Extra_Large' then 1 else 0 end) as mobile_data_xl
        ,sum(case when a.plan_parent = 'Plan_Tier_Mobile_Data' then 1 else 0 end) as mobile_data_other
from service_data_s1 a
LEFT join (select customer_id, location_id, customer_type from account_daily  where calendar_date in (Select Max(calendar_date) from account_daily ) ) b
    on a.customer_id = b.customer_id
LEFT join (select distinct id as location_id, address_id from address_mapping) ak on coalesce(a.location_id,b.location_id) = ak.location_id
LEFT JOIN service_tech c
on a.location_id = c.location_id
group by
ak.address_id
)
,connectivity_household_canvas as (
select distinct
    hh.address_id
    ,consumer_count
    ,consumer_services
    ,business_count
    ,business_services
    ,"State"
    ,city
    ,postal_code
    , latitude
    , longitude
    ,area_type
    ,fixed_service_technology
    ,concat_ws(',',sd.broadband_service,sd.mobile_service,sd.prepaid_service,sd.mobile_broadband) as service_portfolio
    ,concat_ws(','
        ,Case when consumer_count > 0 then 'Consumer' end
        ,Case when business_count > 0 then 'Business' end
        ) as customer_segment
    ,al.location_category as location_classification
    ,service_starter
    ,service_basic
    ,service_advanced
    ,service_premium
    ,service_ultra
    ,service_elite
    ,service_high_speed
    ,service_voice
    ,service_satellite
    ,service_other
    ,prepaid_voice_service
    ,prepaid_data_service
    ,prepaid_alternative
    , prepaid_other
    ,mobile_voice_starter
    ,mobile_voice_basic
    ,mobile_voice_advanced
    ,mobile_voice_premium
    ,mobile_voice_bundle
    ,mobile_other
    ,mobile_data_xs
    ,mobile_data_s
    ,mobile_data_m
    ,mobile_data_l
    ,mobile_data_xl
    ,mobile_data_other
    ,   mobile_voice_starter
        + mobile_voice_basic
        + mobile_voice_advanced
        + mobile_voice_premium
        + mobile_voice_bundle
        + mobile_other
        + mobile_data_xs
        + mobile_data_s
        + mobile_data_m
        + mobile_data_l
        + mobile_data_xl
        + mobile_data_other
        + service_starter
        + service_basic
        + service_advanced
        + service_premium
        + service_ultra
        + service_elite
        + service_high_speed
        + service_satellite
        + service_other
        + service_voice
        + prepaid_voice_service
        + prepaid_data_service
        + prepaid_alternative
        + prepaid_other
        as total_services_per_household
    ,   service_starter
        + service_basic
        + service_advanced
        + service_premium
        + service_ultra
        + service_elite
        + service_high_speed
        + service_satellite
        + service_other
        + service_voice
        as fixed_services_per_household
    ,   mobile_voice_starter
        + mobile_voice_basic
        + mobile_voice_advanced
        + mobile_voice_premium
        + mobile_voice_bundle
        + mobile_data_xs
        + mobile_data_s
        + mobile_data_m
        + mobile_data_l
        + mobile_data_xl
        + mobile_data_other
        + mobile_other
        as mobile_services_per_household
    ,   prepaid_voice_service
        + prepaid_data_service
        + prepaid_alternative
        + prepaid_other
        as prepaid_services_per_household
        ,case
            when new_flag is not null then new_flag
            when active_flag is not null then active_flag
            when inactive_flag is not null then inactive_flag
            when service_starter + service_basic + service_advanced + service_premium + service_ultra + service_elite + service_high_speed + service_satellite + service_other >0 then 'Active_Operator_A'
            else 'Not_With_Operator_A'
        end as broadband_status
from household_profile_canvas hh
left join broadband_active_status fs on fs.address_id = hh.address_id
left join service_data sd on sd.address_id = hh.address_id
left join (select distinct address_id, location_category from address_location) al on al.address_id = hh.address_id
where calendar_date = (select max(calendar_date) from household_profile_canvas)
)
select
    hh.address_id
    ,consumer_count
    ,business_count
    ,consumer_services
    ,business_services
    ,"state"
    ,city
    , postal_code
    , latitude
    , longitude
    ,area_type
    ,fixed_service_technology
    , nf.technology_type as network_tech_type
    ,case
        when service_status is not null then
            case
                when service_status = 'Service_Status_A' then 'Network_Type_A'
                when service_status = 'Service_Status_B' then 'Network_Type_B_Pending'
                when service_status = 'Service_Status_C' then 'Network_Type_C_Not_Installed'
                when service_status = 'Service_Status_D' then 'Network_Type_D_Not_Installed'
                when technology_type = 'Network_Type_A' and service_status = 'Service_Status_E' then 'Network_Type_E_Pending'
                when technology_type = 'Network_Type_A' and service_status = 'Service_Status_F' then 'Network_Type_F_Pending'
                when technology_type = 'Network_Type_A' and service_status = 'Service_Status_G' then 'Network_Type_G_Active'
                when technology_type = 'Network_Type_B' and service_status = 'Service_Status_H' then 'Network_Type_H_Pending'
                when technology_type = 'Network_Type_B' and service_status = 'Service_Status_I' then 'Network_Type_I_Pending'
                when technology_type = 'Network_Type_B' and service_status = 'Service_Status_G' then 'Network_Type_B_Active'
                when service_status = 'Service_Status_J' then 'Network_Type_J_Active'
                when service_status = 'Service_Status_K' then 'Network_Type_K_Pending'
                when service_status = 'Service_Status_L' then 'Network_Type_L_Pending'
                when service_status = 'Service_Status_M' then 'Network_Type_M_Pending'
                when service_status = 'Service_Status_N' then 'Network_Type_N_Not_Installed'
                when service_status = 'Service_Status_O' then 'Network_Type_O_Active'
                when service_status = 'Service_Status_P' then 'Network_Type_P_Pending'
                when service_status = 'Service_Status_Q' then 'Network_Type_Q_Pending'
                when service_status = 'Service_Status_R' then 'Network_Type_R_Not_Serviceable'
                when service_status = 'Network_Pending' then 'Network_Type_Pending'
                when service_status = 'Service_Status_S' then 'Network_Type_S_Active'
                when service_status = 'Service_Status_T' then 'Network_Type_T_Pending'
                when service_status = 'Service_Status_U' then 'Network_Type_U_Pending'
                when service_status = 'Service_Status_V' then 'Network_Type_V_Planned'
                when service_status = 'Service_Status_W' then 'Network_Type_W_Planned'
                when service_status = 'Service_Status_X' then 'Network_Type_X_Planned'
                else service_status
            end
        else 'Not_In_Network_Footprint'
    end as service_class
    ,service_restriction
    ,service_status
    ,service_portfolio
    ,customer_segment
    ,location_classification
    ,case when nf.address_id is not null then 'Network_A' else 'Network_Other' end as network_type
    ,segment_type
    ,segment_group
    ,affluence
    ,household_composition
    ,household_income
    ,head_of_household_age
    ,household_lifestage
    ,child_young_probability
    ,child_teen_probability
    ,service_starter as plan_starter
    ,service_basic as plan_basic
    ,service_advanced as plan_advanced
    ,service_premium as plan_premium
    ,service_ultra
    ,service_elite
    ,service_high_speed
    ,service_voice
    ,service_satellite
    ,service_other
    ,prepaid_voice_service
    ,prepaid_data_service
    ,prepaid_alternative
    ,prepaid_other
    ,mobile_voice_starter
    ,mobile_voice_basic
    ,mobile_voice_advanced
    ,mobile_voice_premium
    ,mobile_voice_bundle
    ,mobile_other
    ,mobile_data_xs
    ,mobile_data_s
    ,mobile_data_m
    ,mobile_data_l
    ,mobile_data_xl
    ,mobile_data_other
    ,total_services_per_household
    ,fixed_services_per_household
    ,mobile_services_per_household
    ,prepaid_services_per_household
    ,broadband_status
from connectivity_household_canvas hh
left join (select distinct address_id, service_status, service_restriction, technology_type from broadband_footprint) nf on nf.address_id = hh.address_id
left join tbl_household_segmentation m on m.address_id = hh.address_id
;

------------------------------------
--------ADDING CLOSEST LOCATION--------
------------------------------------
DROP TABLE IF EXISTS tbl_address_portfolio;

CREATE TEMPORARY TABLE tbl_address_portfolio AS

WITH location_record AS (
    SELECT
        dealer_code
        ,store_name
        ,city
        ,postal_code
        ,state
        ,CAST(longitude AS decimal(20,17)) as store_longitude
        ,CAST(latitude AS decimal(20,17)) as store_latitude
		,CAST(longitude AS decimal(20,17)) - 0.8 AS longitude_offset_min
		,CAST(longitude AS decimal(20,17)) + 0.8 AS longitude_offset_max
		,CAST(latitude AS decimal(20,17)) - 0.8  AS latitude_offset_min
		,CAST(latitude AS decimal(20,17)) + 0.8  AS latitude_offset_max
    FROM
        retail_network
)
, locations_within_range AS (
SELECT
    acc.address_id,
    acc.city,
    acc.state,
    acc.postal_code,
    acc.longitude,
    acc.latitude,
    s.store_name,
    s.dealer_code,
    s.store_longitude,
    s.store_latitude,
	ROUND(
		6371 * 2 * ASIN(
			SQRT(
				pow(SIN(RADIANS((CAST(s.store_latitude AS double) - CAST(acc.latitude AS double)) / 2)), 2)
				+ COS(RADIANS(CAST(acc.latitude AS double))) * COS(RADIANS(CAST(s.store_latitude AS double)))
				* pow(SIN(RADIANS((CAST(s.store_longitude AS double) - CAST(acc.longitude AS double)) / 2)), 2)
			)
		),
		2
	) AS distance_km
FROM tbl_address_portfolio_v1 acc
JOIN location_record s
    ON 1=1
WHERE acc.longitude BETWEEN s.longitude_offset_min AND s.longitude_offset_max
AND acc.latitude BETWEEN s.latitude_offset_min AND s.latitude_offset_max
AND
	ROUND(
		6371 * 2 * ASIN(
			SQRT(
				pow(SIN(RADIANS((CAST(s.store_latitude AS double) - CAST(acc.latitude AS double)) / 2)), 2)
				+ COS(RADIANS(CAST(acc.latitude AS double))) * COS(RADIANS(CAST(s.store_latitude AS double)))
				* pow(SIN(RADIANS((CAST(s.store_longitude AS double) - CAST(acc.longitude AS double)) / 2)), 2)
			)
		),
		2
	) <= 80
)
SELECT
	a.*
	,coalesce(b.store_name, 'No locations within 80km') as nearest_store_name
    ,coalesce(b.dealer_code,'NA') as dealer_code
	,b.flag_within_20km
FROM tbl_address_portfolio_v1 a
LEFT JOIN (
	SELECT distinct
		address_id,
		city,
		state,
		postal_code,
		store_name,
		dealer_code,
		distance_km,
		CASE WHEN distance_km <= 20.00 THEN 1 ELSE 0 END AS flag_within_20km
		,ROW_NUMBER() OVER(PARTITION BY address_id order by distance_km asc) as row_num
	FROM locations_within_range
) b
ON a.address_id = b.address_id
AND b.row_num =1;
```

## Your Analysis Steps

1. **TRACE DATA FLOW**: Follow the pipeline dependency graph from base tables to final outputs.
2. **IDENTIFY FILTER BOUNDARIES**: Where do filters first appear? Can they be pushed earlier in the chain?
3. **MAP BASE TABLE SCANS**: Which base tables are scanned by multiple stages? Can scans be consolidated?
4. **CHECK MATERIALIZATION NECESSITY**: Does each temp table NEED to be materialized, or could it be inlined as a CTE?
5. **WRITE REWRITES**: For each statement you change, produce a component payload in the output format below.

## Output Format

First output a **Modified Logic Tree** for the pipeline, showing which statements and components changed.

Then output a **Component Payload JSON** with one statement entry per pipeline stage you modify.

```json
{
  "spec_version": "1.0",
  "dialect": "<dialect>",
  "rewrite_rules": [
    {"id": "R1", "type": "<transform>", "description": "<what>", "applied_to": ["<stmt.comp>"]}
  ],
  "statements": [
    {
      "target_table": "<table_or_view_name>",
      "change": "modified",
      "components": {
        "<cte_name>": {"type": "cte", "change": "modified", "sql": "<CTE body>", "interfaces": {"outputs": ["col1"], "consumes": []}},
        "main_query": {"type": "main_query", "change": "modified", "sql": "<SELECT>", "interfaces": {"outputs": ["col1"], "consumes": ["<cte_name>"]}}
      },
      "reconstruction_order": ["<cte_name>", "main_query"],
      "assembly_template": "CREATE TABLE <target> AS WITH <cte> AS ({<cte>}) {main_query}"
    }
  ],
  "macros": {},
  "frozen_blocks": [],
  "validation_checks": []
}
```

### Rules
- Tree first — generate the pipeline Logic Tree before writing any SQL
- Each `statements[]` entry targets a specific pipeline stage (must match a CREATE in the script)
- Only include statements you actually change
- Output columns of each stage must remain identical (downstream consumers depend on them)
- Rewrites must be semantically equivalent for ALL downstream consumers, not just the immediate next stage
- No ellipsis — every `sql` value must be complete, executable SQL

After the JSON, explain the overall pipeline optimization:

```
Pipeline changes: <2-4 sentences: what cross-statement optimizations and why>
Expected overall speedup: <estimate>
```

## Pipeline Validation Checklist

- Every modified stage preserves its output schema (column names, types, row semantics)
- Filters pushed upstream do not remove rows that downstream consumers need
- If a temp table is inlined as CTE, ALL consumers of that table must be updated
- Redundant scan consolidation must not change join cardinality
- All literal values preserved exactly
