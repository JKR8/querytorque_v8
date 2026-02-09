------------------------
------HHOLD MOSIAC------
------------------------

-- Ensure TPC-DS extension is loaded (for DuckDB environment)
-- INSTALL tpcds;
-- LOAD tpcds;
-- CALL dsdgen(sf=0.1);

-- MOCK DATA SETUP USING TPC-DS
CREATE OR REPLACE TEMPORARY VIEW HOUSEHOLD_CONNECTIVITY_CANVAS AS
SELECT 
    ca_address_sk AS GNAF_PID,
    c_customer_sk AS sbl_cstr_id,
    ca_state AS "State",
    ca_city AS city,
    ca_zip AS PSTCD,
    ca_gmt_offset AS lttd,
    ca_gmt_offset + 100 AS lngtd,
    'Urban' AS area_type,
    'Residential' AS mb_category,
    CAST('2025-02-10' AS DATE) AS clndr_dt,
    substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) || LPAD((ca_address_sk % 51 + 1)::VARCHAR, 2, '0') AS household_mosaic_type_cd,
    substring('ABCDEFGHIJKLMN', (ca_address_sk % 14) + 1, 1) AS household_mosaic_group_cd,
    (ca_address_sk % 7 + 1) AS household_affluence_cd,
    (ca_address_sk % 6 + 1) AS household_composition_cd,
    (ca_address_sk % 7 + 1) AS household_income_cd,
    (ca_address_sk % 15 + 1) AS household_head_of_household_age_cd,
    (ca_address_sk % 10 + 1) AS household_lifestage_cd,
    (ca_address_sk % 10 + 1) AS household_child_0_10_probability_cd,
    (ca_address_sk % 10 + 1) AS household_child_11_18_probability_cd
FROM customer
JOIN customer_address ON c_current_addr_sk = ca_address_sk;

CREATE OR REPLACE TEMPORARY VIEW internet_canvas_daily AS
SELECT 
    ws_bill_customer_sk AS sbl_cstr_id,
    CASE WHEN ws_item_sk % 3 = 0 THEN '4G/5G Sub 6GHz'
         WHEN ws_item_sk % 3 = 1 THEN 'NBN Fixed Wireless'
         ELSE 'LeoStarlink' END AS tech_type,
    ws_bill_addr_sk AS adbor_id,
    d_date AS clndr_dt,
    i_item_id AS Plan_Prdct_Cd,
    i_product_name AS plan_name,
    CASE WHEN ws_item_sk % 2 = 0 THEN 'C' ELSE 'B' END AS bu_cd,
    ws_order_number AS srvc_srl_num,
    d_date AS srvc_cnnct_dt,
    i_class AS Speed_Prdct_Name,
    i_class AS Speed_tier,
    i_item_id AS Plan_part_num
FROM web_sales
JOIN item ON ws_item_sk = i_item_sk
JOIN date_dim ON ws_sold_date_sk = d_date_sk;

CREATE OR REPLACE TEMPORARY VIEW mobile_canvas_daily AS
SELECT 
    ss_customer_sk AS sbl_cstr_id,
    ss_item_sk AS srvc_srl_num,
    CASE WHEN ss_item_sk % 2 = 0 THEN 'Data Service' ELSE 'Voice' END AS srvc_prdct_name,
    d_date AS clndr_dt,
    CASE WHEN ss_item_sk % 2 = 0 THEN 'C' ELSE 'B' END AS bu_cd,
    CASE WHEN ss_item_sk % 2 = 0 THEN 'Data' ELSE 'Telstra' END AS Srvc_Prdct_Sbtyp,
    i_item_id AS Plan_Prdct_Cd
FROM store_sales
JOIN item ON ss_item_sk = i_item_sk
JOIN date_dim ON ss_sold_date_sk = d_date_sk;

CREATE OR REPLACE TEMPORARY VIEW usage_canvas AS
SELECT 
    ws_order_number AS srvc_srl_num,
    ws_quantity * 1024 AS data_mb_total_vlm,
    d_date AS clndr_dt
FROM web_sales
JOIN date_dim ON ws_sold_date_sk = d_date_sk;

CREATE OR REPLACE TEMPORARY VIEW nbn_footprint AS
SELECT 
    ca_address_sk AS adbor_id,
    ca_address_sk AS gnaf_id,
    ca_address_sk AS nbn_loc_id,
    'FIBRE TO THE NODE' AS tech_type,
    'Serviceable by Copper, Existing Copper Pair in-place active with NBN Co.' AS srvc_class_desc,
    NULL::VARCHAR AS coat_rsn
FROM customer_address;

CREATE OR REPLACE TEMPORARY VIEW ADDRS_keymap AS
SELECT 
    ca_address_sk AS ID,
    ca_address_sk AS GNAF_PID
FROM customer_address;

CREATE OR REPLACE TEMPORARY VIEW XTRNL_DAC_DGBR_COHORT AS
SELECT 
    c_customer_sk AS cac,
    CASE WHEN c_customer_sk % 10 = 0 THEN 'DGBR006' 
         WHEN c_customer_sk % 10 = 1 THEN 'DGBR021'
         ELSE 'OTHER' END AS dgbr_cohort
FROM customer;

CREATE OR REPLACE TEMPORARY VIEW keystone_fbb AS
SELECT 
    ws_bill_customer_sk AS cstmr_id,
    ws_bill_addr_sk AS adbor_id,
    i_item_id AS Plan_part_num,
    i_class AS Speed_tier,
    'ADSL' AS tech_type,
    ws_order_number AS fbb_service_id
FROM web_sales
JOIN item ON ws_item_sk = i_item_sk;

CREATE OR REPLACE TEMPORARY VIEW keystone_voice AS
SELECT 
    cs_bill_customer_sk AS cstmr_id,
    cs_bill_addr_sk AS adbor_id,
    'Yes' AS voice_standalone,
    'ADSL' AS tech_type,
    cs_order_number AS voice_service_id,
    'plan' AS "plan"
FROM catalog_sales;

CREATE OR REPLACE TEMPORARY VIEW accnt_CANVAS_DAILY AS
SELECT 
    c_customer_sk AS sbl_cstr_id,
    c_current_addr_sk AS addrs_adbor_id,
    'C' AS bu_cd,
    CAST('2025-02-10' AS DATE) AS clndr_dt
FROM customer;

CREATE OR REPLACE TEMPORARY VIEW PREPAID_CANVAS_daily AS
SELECT 
    cs_bill_customer_sk AS Sbl_Cstr_Id,
    cs_item_sk AS Srvc_Srl_Num,
    'C' AS BU_Cd,
    'Telstra' AS Srvc_Prdct_Sbtyp,
    i_product_name AS Srvc_Prdct_Name,
    'Plan' AS Plan_Name_parent,
    d_date AS clndr_dt
FROM catalog_sales
JOIN item ON cs_item_sk = i_item_sk
JOIN date_dim ON cs_sold_date_sk = d_date_sk;

CREATE OR REPLACE TEMPORARY VIEW D768325_FIXED_PLAN_GROUPS AS
SELECT 
    i_product_name AS plan_name,
    'Internet Plan Basic' AS plan_name_group
FROM item;

CREATE OR REPLACE TEMPORARY VIEW addrs_gnaf AS
SELECT 
    ca_address_sk AS gnaf_pid,
    'Residential' AS mb_category
FROM customer_address;

CREATE OR REPLACE TEMPORARY VIEW d981967_TRS_RETAIL AS
SELECT 
    s_store_id AS primary_dealer_code,
    s_store_name AS TRR_Store_name,
    s_city AS city,
    s_zip AS Post_code,
    s_state AS state,
    CAST((s_store_sk % 12) + 100 AS DOUBLE) AS LONG,
    CAST((s_store_sk % 24) - 12 AS DOUBLE) AS LAT
FROM store;

-- CREATE MOSAIC
DROP TABLE IF EXISTS d768325_Household_Mosaic;
CREATE TEMPORARY TABLE d768325_Household_Mosaic AS
SELECT
    GNAF_PID
    ,case
        when household_mosaic_type_cd = 'A01' then 'Top of the Ladder'
        when household_mosaic_type_cd = 'A02' then 'Luxury Living'
        when household_mosaic_type_cd = 'A03' then 'Central Prosperity'
        when household_mosaic_type_cd = 'A04' then 'Suburban Esteem'
        when household_mosaic_type_cd = 'B05' then 'Successful Spending'
        when household_mosaic_type_cd = 'B06' then 'Careers & Kids'
        when household_mosaic_type_cd = 'B07' then 'Fruitful Families'
        when household_mosaic_type_cd = 'C08' then 'Rooftops & Careers'
        when household_mosaic_type_cd = 'C09' then 'Elite Alternatives'
        when household_mosaic_type_cd = 'C10' then 'Power Couples'
        when household_mosaic_type_cd = 'D11' then 'Scenic Connection'
        when household_mosaic_type_cd = 'D12' then 'Journeyed Equity'
        when household_mosaic_type_cd = 'D13' then 'Coastal Comfort'
        when household_mosaic_type_cd = 'E14' then 'Spacious Traditions'
        when household_mosaic_type_cd = 'E15' then 'Opulent Designs'
        when household_mosaic_type_cd = 'E16' then 'Hardware & Acreage'
        when household_mosaic_type_cd = 'F17' then 'Determined Suburbans'
        when household_mosaic_type_cd = 'F18' then 'Developing Domestics'
        when household_mosaic_type_cd = 'F19' then 'Striving Scholars'
        when household_mosaic_type_cd = 'G20' then 'Youthful Ambition'
        when household_mosaic_type_cd = 'G21' then 'Emerging Metros'
        when household_mosaic_type_cd = 'G22' then 'Spirit Questers'
        when household_mosaic_type_cd = 'G23' then 'Global Studies'
        when household_mosaic_type_cd = 'H24' then 'Backyards & Mates'
        when household_mosaic_type_cd = 'H25' then 'Prams & Trades'
        when household_mosaic_type_cd = 'H26' then 'Earnest Internationals'
        when household_mosaic_type_cd = 'H27' then 'Township Solos'
        when household_mosaic_type_cd = 'I28' then 'Schools & Bills'
        when household_mosaic_type_cd = 'I29' then 'Middle of the Road'
        when household_mosaic_type_cd = 'I30' then 'Regional Essentials'
        when household_mosaic_type_cd = 'J31' then 'Minerals & Airports'
        when household_mosaic_type_cd = 'J32' then 'Selfless & Hardworking'
        when household_mosaic_type_cd = 'J33' then 'Life in the Slow Lane'
        when household_mosaic_type_cd = 'J34' then 'Country Town Courage'
        when household_mosaic_type_cd = 'K35' then 'Mature Modernites'
        when household_mosaic_type_cd = 'K36' then 'New-found Freedom'
        when household_mosaic_type_cd = 'K37' then 'Realistic Horizons'
        when household_mosaic_type_cd = 'L38' then 'Reset Regionals'
        when household_mosaic_type_cd = 'L39' then 'New-found Life'
        when household_mosaic_type_cd = 'L40' then 'Satellite Battlers'
        when household_mosaic_type_cd = 'L41' then 'Downtown Blues'
        when household_mosaic_type_cd = 'L42' then 'Township Assistance'
        when household_mosaic_type_cd = 'M43' then 'Blue-collar Retirees'
        when household_mosaic_type_cd = 'M44' then 'Staying Put'
        when household_mosaic_type_cd = 'M45' then 'Lonesome Elders'
        when household_mosaic_type_cd = 'M46' then 'Retirement Village'
        when household_mosaic_type_cd = 'M47' then 'Rural Retirement'
        when household_mosaic_type_cd = 'N48' then 'Farming Reliance'
        when household_mosaic_type_cd = 'N49' then 'Outback Comfort'
        when household_mosaic_type_cd = 'N50' then 'Soil & Toil'
        when household_mosaic_type_cd = 'N51' then 'Rustic Isolation'
    end as Mosaic_Type
    ,case
        when household_mosaic_group_cd = 'A' then 'First Class Life'
        when household_mosaic_group_cd = 'B' then 'Comfortable Foundations'
        when household_mosaic_group_cd = 'C' then 'Striving for Status'
        when household_mosaic_group_cd = 'D' then 'Secure Tranquility'
        when household_mosaic_group_cd = 'E' then 'Family Fringes'
        when household_mosaic_group_cd = 'F' then 'Establishing Roots'
        when household_mosaic_group_cd = 'G' then 'Growing Independence'
        when household_mosaic_group_cd = 'H' then 'Middle Blue-collars'
        when household_mosaic_group_cd = 'I' then 'Traditional Pursuits'
        when household_mosaic_group_cd = 'J' then 'True Grit'
        when household_mosaic_group_cd = 'K' then 'Mature Freedom'
        when household_mosaic_group_cd = 'L' then 'Hardship & Perseverance'
        when household_mosaic_group_cd = 'M' then 'Graceful Ageing'
        when household_mosaic_group_cd = 'N' then 'Rural Commitment'
    end as Mosaic_Group
    ,case
        when household_affluence_cd = 1 then 'Low income and assets'
        when household_affluence_cd = 2 then 'Average income and low assets'
        when household_affluence_cd = 3 then 'High income and low assets'
        when household_affluence_cd = 4 then 'Average income and assets'
        when household_affluence_cd = 5 then 'Low incomes and high assets'
        when household_affluence_cd = 6 then 'High incomes and assets'
        when household_affluence_cd = 7 then 'Highest incomes and assets'
    end as Affluence
    ,case
        when household_composition_cd = 1 then 'Families'
        when household_composition_cd = 3 then 'Couple'
        when household_composition_cd = 4 then 'Single Parent'
        when household_composition_cd = 5 then 'Single'
        when household_composition_cd = 6 then 'Homeshares'
    end as Household_Composition
    ,case
        when household_income_cd = 1 then '<=$20,799'
        when household_income_cd = 2 then '$20,800 to $41,599'
        when household_income_cd = 3 then '$41,600 to $64,999'
        when household_income_cd = 4 then '$65,000 to $90,999'
        when household_income_cd = 5 then '$91,000 to $129,999'
        when household_income_cd = 6 then '$130,000 to $181,999'
        when household_income_cd = 7 then '$182,200+'
    end as Household_Income
    ,case
        when household_head_of_household_age_cd = 1 then '18 to 19'
        when household_head_of_household_age_cd = 2 then '20 to 24'
        when household_head_of_household_age_cd = 3 then '25 to 29'
        when household_head_of_household_age_cd = 4 then '30 to 34'
        when household_head_of_household_age_cd = 5 then '35 to 39'
        when household_head_of_household_age_cd = 6 then '40 to 44'
        when household_head_of_household_age_cd = 7 then '45 to 49'
        when household_head_of_household_age_cd = 8 then '50 to 54'
        when household_head_of_household_age_cd = 9 then '55 to 59'
        when household_head_of_household_age_cd = 10 then '60 to 64'
        when household_head_of_household_age_cd = 11 then '65 to 69'
        when household_head_of_household_age_cd = 12 then '70 to 74'
        when household_head_of_household_age_cd = 13 then '75 to 79'
        when household_head_of_household_age_cd = 14 then '80 to 84'
        when household_head_of_household_age_cd = 15 then '85+'
    end as Household_Age
    ,case
        when household_lifestage_cd = 1 then 'Young Families'
        when household_lifestage_cd = 2 then 'Independent Youth'
        when household_lifestage_cd = 3 then 'Maturing Couples and Families'
        when household_lifestage_cd = 4 then 'Maturing Independence'
        when household_lifestage_cd = 5 then 'Established Couples and Families'
        when household_lifestage_cd = 6 then 'Older Couples and Families'
        when household_lifestage_cd = 7 then 'Older Independence'
        when household_lifestage_cd = 8 then 'Elderly Families'
        when household_lifestage_cd = 9 then 'Elderly Couples'
        when household_lifestage_cd = 10 then 'Elderly Singles'
    end as Household_Lifestage
    ,case
        when household_child_0_10_probability_cd = 1 then 'Extremely Low Likelihood'
        when household_child_0_10_probability_cd = 2 then 'Very Low Likelihood'
        when household_child_0_10_probability_cd = 3 then 'Low Likelihood'
        when household_child_0_10_probability_cd = 4 then 'Below Average Likelihood'
        when household_child_0_10_probability_cd = 5 then 'Average Likelihood'
        when household_child_0_10_probability_cd = 6 then 'Above Average Likelihood'
        when household_child_0_10_probability_cd = 7 then 'Moderately High Likelihood'
        when household_child_0_10_probability_cd = 8 then 'High Likelihood'
        when household_child_0_10_probability_cd = 9 then 'Very High Likelihood'
        when household_child_0_10_probability_cd = 10 then 'Extremely High Likelihood'
    end as Child_0_10_Probability
    ,case
        when household_child_11_18_probability_cd = 1 then 'Extremely Low Likelihood'
        when household_child_11_18_probability_cd = 2 then 'Very Low Likelihood'
        when household_child_11_18_probability_cd = 3 then 'Low Likelihood'
        when household_child_11_18_probability_cd = 4 then 'Below Average Likelihood'
        when household_child_11_18_probability_cd = 5 then 'Average Likelihood'
        when household_child_11_18_probability_cd = 6 then 'Above Average Likelihood'
        when household_child_11_18_probability_cd = 7 then 'Moderately High Likelihood'
        when household_child_11_18_probability_cd = 8 then 'High Likelihood'
        when household_child_11_18_probability_cd = 9 then 'Very High Likelihood'
        when household_child_11_18_probability_cd = 10 then 'Extremely High Likelihood'
    end as Child_11_18_Probability
from HOUSEHOLD_CONNECTIVITY_CANVAS
where clndr_dt = (select max(clndr_dt) from HOUSEHOLD_CONNECTIVITY_CANVAS)
;

select * from HOUSEHOLD_CONNECTIVITY_CANVAS
where clndr_dt = (select max(clndr_dt) from HOUSEHOLD_CONNECTIVITY_CANVAS)
LIMIT 100;

--------------------------------------
-------TECH TYPE CHANGE HISTORY-------
--------------------------------------
DROP TABLE IF EXISTS d768325_Tech_Change_History;

-- TECH CHANGE
CREATE TEMPORARY TABLE d768325_Tech_Change_History AS
WITH dataset AS (
        select
        sbl_cstr_id
        , case
            when tech_type = '4G/5G Sub 6GHz' then '5G Fixed Wireless'
            when tech_type = 'NBN Fixed Wireless' then 'NBN FW'
            when tech_type = 'LeoStarlink' then 'Starlink'
            else tech_type
            end as tech_type
        , adbor_id
        , min(clndr_dt) as Min_clndr
        , max(clndr_dt) as Max_clndr
    from internet_canvas_daily
    where clndr_dt >= current_date - interval '180 day'
    group by
        sbl_cstr_id
        ,case
            when tech_type = '4G/5G Sub 6GHz' then '5G Fixed Wireless'
            when tech_type = 'NBN Fixed Wireless' then 'NBN FW'
            when tech_type = 'LeoStarlink' then 'Starlink'
            else tech_type
            end
        , adbor_id
)
SELECT
    count (distinct a.sbl_cstr_Id) as customer_count
    , a.tech_type as Prev_TechType
    , b.tech_type as New_TechType
    ,mosaic_type
    , mosaic_group
    , affluence
    , household_composition
    , household_income
    , household_age
    , Household_Lifestage
    , Child_0_10_Probability
    , Child_11_18_Probability
    , a.max_clndr + ((13 - (extract('dow' from a.max_clndr))) % 7) * interval '1 day' as TechChange_Date
from dataset a
inner join dataset b 
    on a.adbor_id = b.adbor_id
    and a.max_clndr between b.min_clndr - interval '7 day' and b.min_clndr + interval '7 day'
    and a.tech_type <> b.tech_type
left join (select ID as ADBOR_ID, GNAF_PID from ADDRS_keymap group by ID , GNAF_PID) ak on ak.adbor_id = a.adbor_id
LEFT JOIN d768325_Household_Mosaic c
    on ak.gnaf_pid = c.gnaf_pid
group BY
     a.tech_type 
    , b.tech_type 
    ,mosaic_type
    , mosaic_group
    , affluence
    , household_composition
    , household_income
    , household_age
    , Household_Lifestage
    , Child_0_10_Probability
    , Child_11_18_Probability
    , a.max_clndr + ((13 - (extract('dow' from a.max_clndr))) % 7) * interval '1 day'
;

-----------------------------
--------FBB REMAINING--------
-----------------------------

DROP TABLE IF EXISTS d768325_Fixed_Remaining_Profiles;

CREATE TEMPORARY TABLE d768325_Fixed_Remaining_Profiles AS
WITH PriorityAssistance as (
    select distinct cac
    from XTRNL_DAC_DGBR_COHORT
    where dgbr_cohort = 'DGBR006'
)
,regional_remote as (
    select distinct cac, dgbr_cohort
    from XTRNL_DAC_DGBR_COHORT
    where dgbr_cohort in ('DGBR021'
                    ,'DGBR022'
                    ,'DGBR022A'
                    ,'DGBR022B'
                    )
)
,nbn_RFS as (
select DISTINCT
    cstmr_id, ServiceID, plan_name
    ,l.adbor_id, l.tech_type
    ,nbn_loc_id
    ,case
        when coat_rsn is not null then coat_rsn
        when srvc_class_desc is null then 'No NBN TechType Available'
        else srvc_class_desc
    end as HFL_Status
        , mosaic_type
        , mosaic_group
        , affluence
        , household_composition
        , household_income
        , household_age
        , Household_Lifestage
        , Child_0_10_Probability
        , Child_11_18_Probability
    from (
        select cstmr_id
        , adbor_id,
        case
            when Plan_part_num in('XAE00001214','XAE00001114','XAE00001236') then 'FBB Starter'
            when Plan_part_num in('XAE00001237','XAE00001024','XAE00001215','XAE00001050','XAE00001190','XAE00001123'
                    ,'XAE00001019','XAE00001230','XAE00001217') then 'FBB Basic'
            when Plan_part_num in('XAE00001216','XAE00001051','XAE00001026','XAE00001116'
                                ,'XAE00001124','XAE00001189','XAE00001241') then 'FBB Essential'
            when Plan_part_num = 'XAE00001232' THEN
                        CASE
                            when Speed_tier in ('Superfast','Superfast Speed') then 'FBB Superfast'
                            when Speed_tier = 'Ultrafast' then 'FBB Ultrafast'
                            else 'FBB Premium'
                        end
            else 'PLAN'
            end as plan_name
        ,'ADSL - Internet' as tech_type
        , fbb_service_id as ServiceID
    from keystone_fbb
        where tech_type = 'ADSL'
        UNION
    select distinct cstmr_id, adbor_id, "plan", 'PSTN - Voice' as tech_type, voice_service_id
    from keystone_voice
        where voice_standalone = 'Yes'
        and tech_type = 'ADSL'
    ) l
left join nbn_footprint rfs on rfs.adbor_id = l.adbor_id
left join (select ID as ADBOR_ID, GNAF_PID from ADDRS_keymap group by ID , GNAF_PID) ak on ak.adbor_id = RFS.adbor_id
LEFT JOIN d768325_Household_Mosaic c
    on ak.gnaf_pid = c.gnaf_pid
)
select
    count(distinct ServiceID) as Service_Count
    ,count(distinct cstmr_id) as Customer_Count
    ,case when p.cac is not null then 'Priority Assistance' end as PA_Check
    ,case when rr.cac is not null then 'Remote Regional' end as RR_Check
    ,tech_type
    ,HFL_Status
    ,plan_name
        , mosaic_type
        , mosaic_group
        , affluence
        , household_composition
        , household_income
        , household_age
        , Household_Lifestage
        , Child_0_10_Probability
        , Child_11_18_Probability
    from nbn_RFS l
    left join PriorityAssistance p on p.cac = l.cstmr_id
    left join regional_remote rr on rr.cac = l.cstmr_id
    group BY
        case when p.cac is not null then 'Priority Assistance' end
        ,case when rr.cac is not null then 'Remote Regional' end
        ,tech_type
        ,HFL_Status
        ,plan_name
        , mosaic_type
        , mosaic_group
        , affluence
        , household_composition
        , household_income
        , household_age
        , Household_Lifestage
        , Child_0_10_Probability
        , Child_11_18_Probability;

-----------------------------
----------FBB USAGE----------
-----------------------------

DROP TABLE IF EXISTS d768325_Service_Usage_Profile;

CREATE TEMPORARY TABLE d768325_Service_Usage_Profile AS
select
    addrs_adbor_id
    ,srvc_srl_num
    ,Product_Type
    ,case when nf.adbor_id is not null then 'NBN' else 'Non-NBN' end as NBN_Check
    , mosaic_type
    , mosaic_group
    , affluence
    , household_composition
    , household_income
    , household_age
    , Household_Lifestage
    , Child_0_10_Probability
    , Child_11_18_Probability
    ,last_day(ac.clndr_dt) as Month_Value
from accnt_CANVAS_DAILY ac
left join nbn_footprint nf on nf.adbor_id = ac.addrs_adbor_id
inner join (
            select sbl_cstr_id, srvc_srl_num, case when srvc_prdct_name = 'Data Service' then 'MBB' else 'Mobile' end as Product_Type, last_day(clndr_dt) as Month_Value
            from mobile_canvas_daily
            where clndr_dt >= current_date - interval '6 month'
            group by sbl_cstr_id, srvc_srl_num, case when srvc_prdct_name = 'Data Service' then 'MBB' else 'Mobile' end, last_day(clndr_dt)
            UNION
            select sbl_cstr_id, srvc_srl_num, 'Internet' as Product_Type, last_day(clndr_dt) as Month_Value
            from internet_canvas_daily
            where  clndr_dt >= current_date - interval '6 month'
            group by sbl_cstr_id, srvc_srl_num, last_day(clndr_dt)
    ) s on s.sbl_cstr_id = ac.sbl_cstr_Id
    and s.month_value = last_day(ac.clndr_dt)
left join (select ID as ADBOR_ID, GNAF_PID from ADDRS_keymap group by ID , GNAF_PID) ak 
    on ak.adbor_id = AC.ADDRS_ADBOR_ID
LEFT JOIN d768325_Household_Mosaic c
    on ak.gnaf_pid = c.gnaf_pid
where ac.clndr_dt >= current_date - interval '6 month'
group by
    addrs_adbor_id
    ,srvc_srl_num
    ,Product_Type
    ,case when nf.adbor_id is not null then 'NBN' else 'Non-NBN' end
    , mosaic_type
    , mosaic_group
    , affluence
    , household_composition
    , household_income
    , household_age
    , Household_Lifestage
    , Child_0_10_Probability
    , Child_11_18_Probability
    ,last_day(ac.clndr_dt);

DROP TABLE IF EXISTS d768325_Connectivity_Data_Usage;

CREATE TEMPORARY TABLE d768325_Connectivity_Data_Usage AS
select srvc_srl_num
  ,data_mb_total_vlm
  ,monthname(clndr_dt) as Month_Name
  ,clndr_dt as Month_Date
from usage_canvas
where clndr_dt >= current_date - interval '6 month'
and data_mb_total_vlm > 0;

----------------
-- Final Step -- 
----------------
DROP TABLE IF EXISTS d768325_Connectivity_Usage;

CREATE TEMPORARY TABLE d768325_Connectivity_Usage AS
select
  count(distinct addrs_adbor_id) as Address_Count
  ,count(distinct addrs_adbor_id) * SIO_COUNT as SIO_TOTAL
  ,SIO_COUNT
  ,concat_ws(',',MBB,Internet,Mobile) as Product_Holding
  ,CASE
        WHEN MBB_Usage = 0 THEN '0'
        WHEN MBB_Usage < 5 THEN '< 5'
        WHEN MBB_Usage < 10 THEN '5 to 10'
        WHEN MBB_Usage < 20 THEN '10 to 20'
        WHEN MBB_Usage < 30 THEN '20 to 30'
        WHEN MBB_Usage < 40 THEN '30 to 40'
        WHEN MBB_Usage < 50 THEN '40 to 50'
        WHEN MBB_Usage < 100 THEN '50 to 100'
        WHEN MBB_Usage < 150 THEN '100 to 150'
        WHEN MBB_Usage >= 150 THEN '150+'
    END AS MBB_Usage_Bucket
  ,CASE
        WHEN Internet_Usage = 0 THEN '0'
        WHEN Internet_Usage < 50 THEN '< 50'
        WHEN Internet_Usage < 100 THEN '50 to 99'
        WHEN Internet_Usage < 150 THEN '100 to 150'
        WHEN Internet_Usage < 200 THEN '150 to 200'
        WHEN Internet_Usage < 250 THEN '200 to 250'
        WHEN Internet_Usage < 500 THEN '250 to 500'
        WHEN Internet_Usage < 1000 THEN '500 to 1000'
        WHEN Internet_Usage >= 1000 THEN '1000+'
    END AS Internet_Usage_Bucket
  ,CASE
        WHEN Mobile_Usage = 0 THEN '0'
        WHEN Mobile_Usage < 5 THEN '< 5'
        WHEN Mobile_Usage < 10 THEN '5 to 10'
        WHEN Mobile_Usage < 20 THEN '10 to 20'
        WHEN Mobile_Usage < 30 THEN '20 to 30'
        WHEN Mobile_Usage < 40 THEN '30 to 40'
        WHEN Mobile_Usage < 50 THEN '50+'
    END AS Mobile_Usage_Bucket
  ,CASE
        WHEN Total_Usage = 0 THEN '0'
        WHEN Total_Usage < 50 THEN '< 50'
        WHEN Total_Usage < 100 THEN '50 to 99'
        WHEN Total_Usage < 150 THEN '100 to 150'
        WHEN Total_Usage < 200 THEN '150 to 200'
        WHEN Total_Usage < 250 THEN '200 to 250'
        WHEN Total_Usage < 500 THEN '250 to 500'
        WHEN Total_Usage < 1000 THEN '500 to 1000'
        WHEN Total_Usage >= 1000 THEN '1000+'
    END AS Total_Usage_Bucket
    , mosaic_type
    , mosaic_group
    , affluence
    , household_composition
    , household_income
    , household_age
    , Household_Lifestage
    , Child_0_10_Probability
    , Child_11_18_Probability
    , NBN_Check
    , month_name
    , month_date
from (
  select
      addrs_adbor_id
      ,count(distinct c.srvc_srl_num) as SIO_Count
      , max(case when Product_Type = 'MBB' then 'MBB' end) as MBB
      , max(case when Product_Type = 'Internet' then 'Internet' end) as Internet
      , max(case when Product_Type = 'Mobile' then 'Mobile' end) as Mobile
      , sum(case when product_type = 'MBB' then data_mb_total_vlm else 0 end)/1024 as MBB_Usage
      , sum(case when product_type = 'Internet' then data_mb_total_vlm else 0 end)/1024 as Internet_Usage
      , sum(case when product_type = 'Mobile' then data_mb_total_vlm else 0 end)/1024 as Mobile_Usage
      , sum(data_mb_total_vlm)/1024 AS Total_Usage
      , NBN_Check
      , month_name
      , month_date
    , mosaic_type
    , mosaic_group
    , affluence
    , household_composition
    , household_income
    , household_age
    , Household_Lifestage
    , Child_0_10_Probability
    , Child_11_18_Probability
  from d768325_Service_Usage_Profile c
  left join d768325_Connectivity_Data_Usage d on d.srvc_srl_num = c.srvc_srl_num
    and d.month_date = c.month_value
  group by addrs_adbor_id
  , NBN_Check
  , month_name
  , month_date    
    , mosaic_type
    , mosaic_group
    , affluence
    , household_composition
    , household_income
    , household_age
    , Household_Lifestage
    , Child_0_10_Probability
    , Child_11_18_Probability
) total_Usage
group by SIO_COUNT
  ,concat_ws(',',MBB,Internet,Mobile)
  ,CASE
        WHEN MBB_Usage = 0 THEN '0'
        WHEN MBB_Usage < 5 THEN '< 5'
        WHEN MBB_Usage < 10 THEN '5 to 10'
        WHEN MBB_Usage < 20 THEN '10 to 20'
        WHEN MBB_Usage < 30 THEN '20 to 30'
        WHEN MBB_Usage < 40 THEN '30 to 40'
        WHEN MBB_Usage < 50 THEN '40 to 50'
        WHEN MBB_Usage < 100 THEN '50 to 100'
        WHEN MBB_Usage < 150 THEN '100 to 150'
        WHEN MBB_Usage >= 150 THEN '150+'
    END
  ,CASE
        WHEN Internet_Usage = 0 THEN '0'
        WHEN Internet_Usage < 50 THEN '< 50'
        WHEN Internet_Usage < 100 THEN '50 to 99'
        WHEN Internet_Usage < 150 THEN '100 to 150'
        WHEN Internet_Usage < 200 THEN '150 to 200'
        WHEN Internet_Usage < 250 THEN '200 to 250'
        WHEN Internet_Usage < 500 THEN '250 to 500'
        WHEN Internet_Usage < 1000 THEN '500 to 1000'
        WHEN Internet_Usage >= 1000 THEN '1000+'
    END
  ,CASE
        WHEN Mobile_Usage = 0 THEN '0'
        WHEN Mobile_Usage < 5 THEN '< 5'
        WHEN Mobile_Usage < 10 THEN '5 to 10'
        WHEN Mobile_Usage < 20 THEN '10 to 20'
        WHEN Mobile_Usage < 30 THEN '20 to 30'
        WHEN Mobile_Usage < 40 THEN '30 to 40'
        WHEN Mobile_Usage < 50 THEN '50+'
    END
  ,CASE
        WHEN Total_Usage = 0 THEN '0'
        WHEN Total_Usage < 50 THEN '< 50'
        WHEN Total_Usage < 100 THEN '50 to 99'
        WHEN Total_Usage < 150 THEN '100 to 150'
        WHEN Total_Usage < 200 THEN '150 to 200'
        WHEN Total_Usage < 250 THEN '200 to 250'
        WHEN Total_Usage < 500 THEN '250 to 500'
        WHEN Total_Usage < 1000 THEN '500 to 1000'
        WHEN Total_Usage >= 1000 THEN '1000+'
    END
    , NBN_Check
    , month_name
    , month_date
    , mosaic_type
    , mosaic_group
    , affluence
    , household_composition
    , household_income
    , household_age
    , Household_Lifestage
    , Child_0_10_Probability
    , Child_11_18_Probability;

--------------------------------
----ADDRESS PRODUCT HOLDINGS----
--------------------------------
DROP TABLE IF EXISTS d768325_Address_Profiles_S1;

CREATE TEMPORARY TABLE d768325_Address_Profiles_S1 AS

with internet_canvas as (
 select
        a.sbl_cstr_id
        ,a.srvc_srl_num
        ,a.Plan_Prdct_Cd
        ,a.plan_name
        ,CASE WHEN a.adbor_id is not null then a.tech_type else null end as service_tech_Type
        , a.adbor_id
        , ak.gnaf_pid
        , a.bu_cd
, CASE 
    WHEN b.plan_name_group ='Internet Plan Starter' then 'FBB Starter'
    WHEN b.plan_name_group ='Internet Plan Basic' then 'FBB Basic'
    WHEN b.plan_name_group ='Internet Plan Essential' then 'FBB Essential'
    WHEN b.plan_name_group ='5G Home Internet' then 'FBB_5G'
    WHEN b.plan_name_group ='Starlink' then 'FBB_Strlnk'
    when Plan_Prdct_Cd = 'XAE00001232' THEN
                    CASE
                        when Speed_Prdct_Name in ('Superfast','Superfast Speed') then 'FBB Superfast'
                        when Speed_Prdct_Name = 'Ultrafast' then 'FBB Ultrafast'
                        else 'FBB Premium'
                    end
    when Plan_Prdct_Cd = 'FSUBBDL-BXLG' then 'FBB Superfast'
    when plan_prdct_cd = 'FSUBBDL-BXXL' then 'FBB Ultrafast'
    WHEN b.plan_name_group ='Internet Plan Premium' then 'FBB Premium'
    WHEN b.plan_name_group ='Home Phone Plan'  THEN 'FBB HomePhone'
WHEN b.plan_name_group in ('Out of Market Plan','Foxtel Bundle','Internet Trial Plan','Small Business Bundle','Bigpond Data plan') THEN 'Internet_Other'
ELSE 'Internet_Other'
    end as Plan_name_parent
from internet_canvas_daily a
LEFT JOIN D768325_FIXED_PLAN_GROUPS b
on a.plan_name = b.plan_name
left join (select ID as ADBOR_ID, GNAF_PID from ADDRS_keymap group by ID , GNAF_PID) ak on ak.adbor_id = a.adbor_id
where clndr_dt = (select max(clndr_dt) from internet_canvas_daily)
and BU_CD in ('B','C')
and ak.gnaf_pid is not null
group by 
 a.sbl_cstr_id
        ,a.srvc_srl_num
        ,a.Plan_Prdct_Cd
        ,a.plan_name
        , a.tech_type 
        , a.adbor_id
        , ak.gnaf_pid
        , a.bu_cd
, CASE 
    WHEN b.plan_name_group ='Internet Plan Starter' then 'FBB Starter'
    WHEN b.plan_name_group ='Internet Plan Basic' then 'FBB Basic'
    WHEN b.plan_name_group ='Internet Plan Essential' then 'FBB Essential'
    WHEN b.plan_name_group ='5G Home Internet' then 'FBB_5G'
    WHEN b.plan_name_group ='Starlink' then 'FBB_Strlnk'
    when Plan_Prdct_Cd = 'XAE00001232' THEN
                    CASE
                        when Speed_Prdct_Name in ('Superfast','Superfast Speed') then 'FBB Superfast'
                        when Speed_Prdct_Name = 'Ultrafast' then 'FBB Ultrafast'
                        else 'FBB Premium'
                    end
    when Plan_Prdct_Cd = 'FSUBBDL-BXLG' then 'FBB Superfast'
    when plan_prdct_cd = 'FSUBBDL-BXXL' then 'FBB Ultrafast'
    WHEN b.plan_name_group ='Internet Plan Premium' then 'FBB Premium'
    WHEN b.plan_name_group ='Home Phone Plan'  THEN 'FBB HomePhone'
WHEN b.plan_name_group in ('Out of Market Plan','Foxtel Bundle','Internet Trial Plan','Small Business Bundle','Bigpond Data plan') THEN 'Internet_Other'
ELSE 'Internet_Other'
    end 
)
,mobile_canvas as (        
            select          
          Sbl_Cstr_Id  
          ,Srvc_Srl_Num  
          ,BU_CD            
          ,case when Srvc_Prdct_Sbtyp = 'Data' then 'MBB' else 'HH' end as Plan_Type_Cat
          ,case                
            when Plan_Prdct_Cd in ('VMP0000192','VMP0000209','VMP0000220','MSUBVOD-VXSM','VMP0000225','MSUBVOD-VXSM-V02') then 'DV HH Starter'  
            when Plan_Prdct_Cd in ('VMP0000193','VMP0000210','MSUBVOD-VSML','VMP0000226','MSUBVOD-VSML-V02') then 'DV HH Basic'            
            when Plan_Prdct_Cd in ('VMP0000194','VMP0000211','MSUBVOD-VMED','VMP0000223','MSUBVOD-VMED-V02') then 'DV HH Essential'            
            when Plan_Prdct_Cd in ('VMP0000195','VMP0000212','MSUBVOD-VLRG','VMP0000224','MSUBVOD-VLRG-V02') then 'DV HH Premium'              
            WHEN Plan_Prdct_Cd IN('VMP0000227','MSUBVOD-VCMP') THEN 'DV HH Bdl'            
            when Plan_Prdct_Cd in ('VMP0000196','MSUBDAT-DXSM') then 'DV MBB XS'                
            when Plan_Prdct_Cd in ('VMP0000197','MSUBDAT-DSML') then 'DV MBB S'            
            when Plan_Prdct_Cd in ('VMP0000198','VMP0000221','MSUBDAT-DMED','MSUBDAT-DMED-V02') then 'DV MBB M'            
            when Plan_Prdct_Cd in ('VMP0000200','VMP0000222','MSUBDAT-DLRG','MSUBDAT-DLRG-V02') then 'DV MBB L'            
            WHEN Plan_Prdct_Cd IN('VMP0000228','MSUBDAT-DCMP') THEN 'DV MBB Bdl'       
            WHEN Srvc_Prdct_Sbtyp = 'Data' THEN 'MBB_Other'
            else 'Mobile_Other'            
          end as Plan_name_parent
    from mobile_canvas_daily m          
          where clndr_dt in (Select Max(clndr_dt) from mobile_canvas_daily )  
        and BU_CD in ('C','B')
)
,prepaid_canvas as (
        select distinct
            Sbl_Cstr_Id
            ,Srvc_Srl_Num  
            ,BU_Cd        
            ,case
                when Srvc_Prdct_Sbtyp = 'Telstra' and Srvc_Prdct_Name not like '%Broadband%' then 'PrepTelstra_HH'
                when Srvc_Prdct_Sbtyp = 'Telstra' and Srvc_Prdct_Name like '%Broadband%' then 'PrepTelstra_MBB'        
                when Srvc_Prdct_Sbtyp = 'Boost' and Srvc_Prdct_Name not like '%Broadband%' then 'PrepBoost_HH'
                else 'Prepaid Other'
            end as Plan_Name_parent  
    from PREPAID_CANVAS_daily            
        where clndr_dt = (select max(Clndr_Dt) from PREPAID_CANVAS_daily)            
        and BU_Cd in('C','B')
)
,fbb_canvas as (
    select
        GNAF_PID
        ,MAX(case when ic2.srvc_cnnct_dt >= current_date - interval '90 day' then 'New to Telstra (>= 90 days)' end) as New_Active
        ,MAX(case when ic2.adbor_id is not null then 'Active Telstra' end) as Current_Active
        ,MAX(case when ic2.adbor_id is null then 'Left Telstra (<= 90 days)' end) as Previously_Active
    from internet_canvas_daily ic1
    left join (select  ID as ADBOR_ID, GNAF_PID from ADDRS_keymap group by ID, GNAF_PID) ak on ak.adbor_id = ic1.adbor_id
    left join (
            select distinct
                ic2.adbor_id
                ,srvc_cnnct_dt
            from internet_canvas_daily ic2
            where clndr_dt = (select max(clndr_dt) from internet_canvas_daily)
            and BU_CD in ('C','B')
        ) ic2 on ic1.adbor_id = ic2.adbor_id
    where clndr_dt >= current_date - interval '90 day'
    and BU_CD in ('C','B')
    group by
        GNAF_PID
)
,service_data_s1 as (
select sbl_Cstr_id, srvc_srl_num,bu_cd, plan_name,service_tech_type, adbor_id, plan_name_parent, null::VARCHAR as plan_type_Cat, 'Internet_Canvas' as Source from internet_canvas
UNION ALL
select sbl_cstr_id, srvc_srl_num, bu_cd, null, null, null, plan_name_parent, null, 'PrePaid_Canvas'  from prepaid_canvas
UNION ALL
select sbl_Cstr_id, srvc_srl_num, bu_cd, null, null, null,plan_name_parent, plan_type_Cat, 'Mobile_Canvas' from mobile_canvas
)
, service_tech as (
    select distinct service_tech_type, adbor_id from service_data_s1 )
,service_data as(
select 
ak.GNAF_PID 
,  CASE WHEN ak.GNAF_PID IS NOT NULL THEN string_agg(c.service_tech_type, ', ' ORDER BY c.service_tech_type) ELSE NULL END as fixed_service_tech_type
        ,MAX(case when a.source = 'Internet_Canvas' then 'Internet' end) as Internet 
        ,MAX(case when a.source = 'Mobile_Canvas' then 'Mobile' end) as Mobile
        ,MAX(case when a.source = 'PrePaid_Canvas' then 'PrePaid' end) as PrePaid
        ,MAX(case when a.Plan_Type_Cat = 'Data' then 'MBB' end) as MBB
        ,COUNT(DISTINCT case when a.bu_cd = 'C' then a.sbl_Cstr_id else null end) as Consumer_Count
        ,COUNT(DISTINCT case when a.bu_cd = 'B' then a.sbl_Cstr_id else null end) as Business_Count
        ,COUNT(DISTINCT case when a.bu_cd = 'C' then a.srvc_srl_num else null end) as Consumer_Services
        ,COUNT(DISTINCT case when a.bu_cd = 'B' then a.srvc_srl_num else null end) as Business_Services
        ,sum(case when a.plan_name_parent = 'FBB Starter' then 1 else 0 end) as Internet_DVS
        ,sum(case when a.plan_name_parent = 'FBB Basic' then 1 else 0 end) as Internet_DVB
        ,sum(case when a.plan_name_parent = 'FBB Essential' then 1 else 0 end) as Internet_DVE
        ,sum(case when a.plan_name_parent = 'FBB Premium' then 1 else 0 end) as Internet_DVP
        ,sum(case when a.plan_name_parent = 'FBB Superfast' then 1 else 0 end) as Internet_DVU
        ,sum(case when a.plan_name_parent = 'FBB Ultrafast' then 1 else 0 end) as Internet_DVUplus
        ,sum(case when a.plan_name_parent = 'FBB_5G' then 1 else 0 end) as Internet_fiveg
         ,sum(case when a.plan_name_parent = 'FBB_Strlnk' then 1 else 0 end) as Internet_strlnk                 
        ,sum(case when a.plan_name_parent = 'FBB HomePhone' then 1 else 0 end) as Internet_homep
        ,sum(case when a.plan_name_parent = 'Internet_Other' then 1 else 0 end) as Internet_Other              
        ,sum(case when a.plan_name_parent = 'PrepTelstra_HH' then 1 else 0 end) as PrepTelstra_HH          
        ,sum(case when a.plan_name_parent = 'PrepTelstra_MBB' then 1 else 0 end) as PrepTelstra_MBB            
        ,sum(case when a.plan_name_parent = 'PrepBoost_HH' then 1 else 0 end) as PrepBoost_HH
        ,sum(case when a.plan_name_parent = 'Prepaid Other' then 1 else 0 end) as Prep_Other                   
        ,sum(case when a.plan_name_parent = 'DV HH Starter' then 1 else 0 end) as Mobile_DVHHS
        ,sum(case when a.plan_name_parent = 'DV HH Basic' then 1 else 0 end) as Mobile_DVHHB              
        ,sum(case when a.plan_name_parent = 'DV HH Essential' then 1 else 0 end) as Mobile_DVHHE              
        ,sum(case when a.plan_name_parent = 'DV HH Premium' then 1 else 0 end) as Mobile_DVHHP            
        ,sum(case when a.plan_name_parent = 'DV HH Bdl' then 1 else 0 end) as Mobile_DVHHBdl      
        ,sum(case when a.plan_name_parent = 'Mobile_Other' then 1 else 0 end) as Mobile_Other                 
        ,sum(case when a.plan_name_parent = 'DV MBB XS' then 1 else 0 end) as Mobile_DVMBBXS              
        ,sum(case when a.plan_name_parent = 'DV MBB S' then 1 else 0 end) as Mobile_DVMBBS            
        ,sum(case when a.plan_name_parent = 'DV MBB M' then 1 else 0 end) as Mobile_DVMBBM            
        ,sum(case when a.plan_name_parent = 'DV MBB L' then 1 else 0 end) as Mobile_DVMBBL            
        ,sum(case when a.plan_name_parent = 'DV MBB Bdl' then 1 else 0 end) as Mobile_DVMBBBdl
        ,sum(case when a.plan_name_parent = 'MBB_Other' then 1 else 0 end) as MBB_Other     
from service_data_s1 a
LEFT join (select sbl_Cstr_id, addrs_adbor_id, BU_CD from accnt_CANVAS_DAILY  where clndr_dt in (Select Max(clndr_dt) from accnt_CANVAS_DAILY ) ) b
    on a.sbl_Cstr_id = b.sbl_cstr_id
LEFT join (select distinct ID as ADBOR_ID, GNAF_PID from ADDRS_keymap) ak on coalesce(a.adbor_id,b.addrs_adbor_id) = ak.adbor_id
LEFT JOIN service_tech c 
on a.adbor_id = c.adbor_id
group by
ak.GNAF_PID 
)
,connectivity_household_canvas as (
select distinct
    hh.GNAF_PID
    ,Consumer_Count
    ,Consumer_Services
    ,Business_Count
    ,Business_Services
    ,"State"
    ,city
    ,PSTCD
    , lttd
    , lngtd
    ,area_type
    ,fixed_service_tech_type
    ,concat_ws(',',sd.Internet,sd.Mobile,sd.PrePaid,sd.MBB) as Product_Holdings
    ,concat_ws(','
        ,Case when Consumer_Count > 0 then 'Consumer' end
        ,Case when Business_Count > 0 then 'Business' end
        ) as Business_Holdings
    ,ag.mb_category as MeshBlock_Category
    ,Internet_DVS
    ,Internet_DVB
    ,Internet_DVE
    ,Internet_DVP
    ,Internet_DVU
    ,Internet_DVUplus
    ,Internet_fiveg
    ,Internet_homep
    ,Internet_strlnk            
    ,Internet_Other             
    ,PrepTelstra_HH
    ,PrepTelstra_MBB
    ,PrepBoost_HH
    , Prep_Other               
    ,Mobile_DVHHS
    ,Mobile_DVHHB
    ,Mobile_DVHHE
    ,Mobile_DVHHP
    ,Mobile_DVHHBdl
    ,Mobile_Other               
    ,Mobile_DVMBBXS
    ,Mobile_DVMBBS
    ,Mobile_DVMBBM
    ,Mobile_DVMBBL
    ,Mobile_DVMBBBdl
    ,MBB_Other                  
    ,   Mobile_DVHHS
        + Mobile_DVHHB
        + Mobile_DVHHE
        + Mobile_DVHHP
        + Mobile_DVHHBdl
        + Mobile_Other
        + Mobile_DVMBBXS
        + Mobile_DVMBBS
        + Mobile_DVMBBM
        + Mobile_DVMBBL
        + Mobile_DVMBBBdl
        + MBB_Other                  
        + Internet_DVS
        + Internet_DVB
        + Internet_DVE
        + Internet_DVP
        + Internet_DVU
        + Internet_DVUplus
        + Internet_fiveg
        + Internet_strlnk            
        + Internet_Other               
        + Internet_homep
        + PrepTelstra_HH
        + PrepTelstra_MBB
        + PrepBoost_HH
        + Prep_Other 
        as SIOs_per_Household
    ,   Internet_DVS
        + Internet_DVB
        + Internet_DVE
        + Internet_DVP
        + Internet_DVU
        + Internet_DVUplus
        + Internet_fiveg
        + Internet_strlnk           
        + Internet_Other             
        + Internet_homep
        as Fixed_SIOs_per_Household 
    ,   Mobile_DVHHS
        + Mobile_DVHHB
        + Mobile_DVHHE
        + Mobile_DVHHP
        + Mobile_DVHHBdl
        + Mobile_DVMBBXS
        + Mobile_DVMBBS
        + Mobile_DVMBBM
        + Mobile_DVMBBL
        + Mobile_DVMBBBdl
        + MBB_Other                 
        + Mobile_Other
        as MObile_SIOs_per_Household
    ,   PrepTelstra_HH
        + PrepTelstra_MBB
        + PrepBoost_HH
        + Prep_Other 
        as Prepaid_SIOs_per_Household 
        ,case
            when New_Active is not null then New_Active
            when Current_Active is not null then Current_Active
            when Previously_Active is not null then Previously_Active
            when Internet_DVS + Internet_DVB + Internet_DVE + Internet_DVP + Internet_DVU + Internet_DVUplus + Internet_fiveg + Internet_strlnk + Internet_Other >0 then 'Active Telstra'  
            else 'Not with Telstra'
        end as FBB_Status
from HOUSEHOLD_CONNECTIVITY_CANVAS hh
left join fbb_canvas FS on fs.gnaf_pid = hh.gnaf_pid
left join service_data sd on sd.GNAF_PID = hh.GNAF_PID
left join (select distinct gnaf_pid, mb_category from addrs_gnaf) ag on ag.gnaf_pid = hh.gnaf_pid
where clndr_dt = (select max(clndr_dt) from HOUSEHOLD_CONNECTIVITY_CANVAS)
)
select
    hh.GNAF_PID
    ,Consumer_Count
    ,Business_Count
    ,Consumer_Services
    ,Business_Services
    ,"state"
    ,city
    , pstcd
    , lttd
    , lngtd
    ,area_type
    ,fixed_service_tech_type
    , nf.tech_type as ADDRESS_TECH_TYPE
    ,case
        when srvc_class_desc is not null then
            case
                when srvc_class_desc = 'Serviceable by FTTC, drop in place, DPU cut in complete, service had been activated' then 'NBN FTTC'
                when srvc_class_desc = 'Serviceable by FTTC, drop in place, but cut in at DPU is required' then 'NBN FTTC - Not Installed'
                when srvc_class_desc = 'Serviceable by FTTC, drop in place, DPU cut in complete, service had not been activated yet' then 'NBN FTTC - Not Installed'
                when srvc_class_desc = 'Serviceable by FTTC but there is no copper path / lead in yet available' then 'NBN FTTC - Not Installed'
                when tech_type = 'FIBRE TO THE NODE' and srvc_class_desc = 'Serviceable by Copper, Existing Copper Pair in-place not active with NBN Co.' then 'NBN FTTN - Not Installed'
                when tech_type = 'FIBRE TO THE NODE' and srvc_class_desc = 'Serviceable by Copper, no existing Copper Pair in-place, lead-in required' then 'NBN FTTN - Not Installed'
                when tech_type = 'FIBRE TO THE NODE' and srvc_class_desc = 'Serviceable by Copper, Existing Copper Pair in-place active with NBN Co.' then 'NBN FTTN'
                when tech_type = 'FIBRE TO THE BUILDING' and srvc_class_desc = 'Serviceable by Copper, Existing Copper Pair in-place not active with NBN Co.' then 'NBN FTTB - Not Installed'
                when tech_type = 'FIBRE TO THE BUILDING' and srvc_class_desc = 'Serviceable by Copper, no existing Copper Pair in-place, lead-in required' then 'NBN FTTB - Not Installed'
                when tech_type = 'FIBRE TO THE BUILDING' and srvc_class_desc = 'Serviceable by Copper, Existing Copper Pair in-place active with NBN Co.' then 'NBN FTTB'
                when srvc_class_desc = 'Serviceable by fibre, Drop and NTD in place' then 'NBN FTTP'
                when srvc_class_desc = 'Serviceable by fibre, no Drop in place, no NTD' then 'NBN FTTP - Not Installed'
                when srvc_class_desc = 'Serviceable by fibre, Drop in place, no NTD' then 'NBN FTTP - Not Installed'
                when srvc_class_desc = 'Serviceable by Wireless, NTD installed' then 'NBN FW'
                when srvc_class_desc = 'Serviceable by Wireless, NTD not installed' then 'NBN FW - Not Installed'
                when srvc_class_desc = 'Premises within HFC footprint, Drop, wall plate and NTD in place' then 'NBN HFC'
                when srvc_class_desc = 'Premises within HFC footprint, drop and wall plate in place no NTD' then 'NBN HFC - Not Installed'
                when srvc_class_desc = 'Premises within HFC footprint, drop in place, no wall plate or NTD' then 'NBN HFC - Not Installed'
                when srvc_class_desc = 'Premises within HFC footprint, no drop, wall plate or NTD' then 'NBN HFC - Not Installed'
                when srvc_class_desc = 'Premises within HFC footprint, but not serviceable.' then 'NBN HFC - Not Installed'
                when srvc_class_desc = 'NBN Co has not yet determined the NBN Co Network footprint that will apply to this address' then 'NBN Not Yet Determined'
                when srvc_class_desc = 'Serviced by satellite (dish and NTD in place)' then 'NBN Sat'
                when srvc_class_desc = 'Serviceable by satellite but no satellite dish / NTD yet in place' then 'NBN Sat - Not Installed'
                when srvc_class_desc = 'Planned to be served by FTTC (as yet not serviceable)' then 'Planned to be NBN FTTC'
                when srvc_class_desc = 'Planned to be serviced by Copper (as yet not serviceable)' then 'Planned to be NBN FTTN'
                when srvc_class_desc = 'Planned to be serviced by fibre (as yet not serviceable)' then 'Planned to be NBN FTTP'
                else srvc_class_desc
            end
        else 'Not in the NBN Footprint'
    end as Service_Class
    ,coat_rsn
    ,srvc_class_desc
    ,Product_Holdings
    ,Business_Holdings
    ,MeshBlock_Category
    ,case when nf.gnaf_id is not null then 'NBN' else 'Non-NBN' end as NBN_Type
    ,Mosaic_Type
    ,Mosaic_Group
    ,Affluence
    ,Household_Composition
    ,Household_Income
    ,Household_Age
    ,Household_Lifestage
    ,Child_0_10_Probability
    ,Child_11_18_Probability
    ,Internet_DVS as FBB_Starter
    ,Internet_DVB as FBB_Basic
    ,Internet_DVE as FBB_Essential
    ,Internet_DVP as FBB_Premium
    ,Internet_DVU as FBB_Superfast
    ,Internet_DVUplus
    ,Internet_fiveg
    ,Internet_homep
    ,Internet_strlnk
    ,Internet_Other
    ,PrepTelstra_HH
    ,PrepTelstra_MBB
    ,PrepBoost_HH
    ,Prep_Other
    ,Mobile_DVHHS
    ,Mobile_DVHHB
    ,Mobile_DVHHE
    ,Mobile_DVHHP
    ,Mobile_DVHHBdl
    ,Mobile_Other
    ,Mobile_DVMBBXS
    ,Mobile_DVMBBS
    ,Mobile_DVMBBM
    ,Mobile_DVMBBL
    ,Mobile_DVMBBBdl
    ,MBB_Other
    ,SIOs_per_Household
    ,Fixed_SIOs_per_Household
    ,MObile_SIOs_per_Household
    ,Prepaid_SIOs_per_Household
    ,FBB_Status
from connectivity_household_canvas hh
left join (select distinct gnaf_id, srvc_class_desc, coat_rsn, tech_type from nbn_footprint) nf on nf.GNAF_Id = hh.GNAF_PID
left join d768325_Household_Mosaic m on m.gnaf_pid = hh.gnaf_pid
;

------------------------------------
--------ADDING CLOSEST STORE--------
------------------------------------
DROP TABLE IF EXISTS d768325_Address_Profiles;

CREATE TEMPORARY TABLE d768325_Address_Profiles AS

WITH Store_Record AS (
    SELECT
        primary_dealer_code
        ,TRR_Store_name
        ,city
        ,Post_code
        ,state
        ,CAST(LONG AS decimal(20,17)) as store_long
        ,CAST(LAT AS decimal(20,17)) as store_lat
		,CAST(LONG AS decimal(20,17)) - 0.8 AS longitude_offset_L
		,CAST(LONG AS decimal(20,17)) + 0.8 AS longitude_offset_H
		,CAST(LAT AS decimal(20,17)) - 0.8  AS latitude_offset_L
		,CAST(LAT AS decimal(20,17)) + 0.8  AS latitude_offset_H
    FROM
        d981967_TRS_RETAIL 
)
, all_under80 AS (
SELECT
    acc.gnaf_pid,
    acc.city,
    acc.state,
    acc.pstcd,
    acc.lngtd,
    acc.lttd,
    s.TRR_Store_name,
    s.primary_dealer_code,
    s.store_long,
    s.store_lat,
	ROUND(
		6371 * 2 * ASIN(
			SQRT(
				pow(SIN(RADIANS((CAST(s.store_lat AS double) - CAST(acc.lttd AS double)) / 2)), 2)
				+ COS(RADIANS(CAST(acc.lttd AS double))) * COS(RADIANS(CAST(s.store_lat AS double)))
				* pow(SIN(RADIANS((CAST(s.store_long AS double) - CAST(acc.lngtd AS double)) / 2)), 2)
			)
		),
		2
	) AS distance_km
FROM d768325_Address_Profiles_s1 acc
JOIN Store_Record s
    ON 1=1
WHERE acc.lngtd BETWEEN s.longitude_offset_L AND s.longitude_offset_H
AND acc.lttd BETWEEN s.latitude_offset_L AND s.latitude_offset_H
AND
	ROUND(
		6371 * 2 * ASIN(
			SQRT(
				pow(SIN(RADIANS((CAST(s.store_lat AS double) - CAST(acc.lttd AS double)) / 2)), 2)
				+ COS(RADIANS(CAST(acc.lttd AS double))) * COS(RADIANS(CAST(s.store_lat AS double)))
				* pow(SIN(RADIANS((CAST(s.store_long AS double) - CAST(acc.lngtd AS double)) / 2)), 2)
			)
		),
		2
	) <= 80
) 
SELECT 
	a.*
	,coalesce(b.TRR_Store_name, 'No stores within 80km') as TRR_Store_name
    ,coalesce(b.primary_dealer_code,'NA') as primary_dealer_code
	,b.Flag20KmDistance
FROM d768325_Address_Profiles_S1 a
LEFT JOIN (
	SELECT distinct
		gnaf_pid,
		city,
		state,
		pstcd,
		TRR_Store_name,
		primary_dealer_code,
		distance_km,
		CASE WHEN distance_km <= 20.00 THEN 1 ELSE 0 END AS Flag20KmDistance
		,ROW_NUMBER() OVER(PARTITION BY gnaf_pid order by distance_km asc) as rownum
	FROM all_under80
) b
ON a.GNAF_PID = b.GNAF_PID
AND b.rownum =1;
