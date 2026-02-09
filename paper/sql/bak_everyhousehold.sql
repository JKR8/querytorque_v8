------------------------
------HHOLD MOSIAC------
------------------------
--- NOTE AA-COE HAVE ADVISED THERE WILL BE CHANGES TO THE MOSIAC TABLES IN MARCH 2025 AND WE WILL MOST LIKELY NEED TO UPDATE THE STATEMENTS IN ANY MOSAIC TABLES TO REFLECT THE NEW ATTRIBUTES

--CREATE MOSAIC
IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'd768325_HHOLD_MOSAIC' AND schema_id = SCHEMA_ID('DATAMART_BIDWR_CI_dev_DATA'))     DROP TABLE DATAMART_BIDWR_CI_dev_DATA.d768325_HHOLD_MOSAIC;

SELECT
    GNAF_PID
    ,case
        when experian_mosaic_type_cd = 'A01' then 'Top of the Ladder' --Wealthiest families, married couples often with adult children,owning very expensive properties in exclusive inner-urban areas
        when experian_mosaic_type_cd = 'A02' then 'Luxury Living' --Baby boomer families, often with adult children, owningexpensive properties in inner-urban & coastal areas
        when experian_mosaic_type_cd = 'A03' then 'Central Prosperity' --Middle-older aged empty nester couples renting very expensiveproperties in inner-urban areas, with high income
        when experian_mosaic_type_cd = 'A04' then 'Suburban Esteem' --Traditional baby boomer couples with adult children, owningexpensive properties in inner-urban & suburban areas of Sydney
        when experian_mosaic_type_cd = 'B05' then 'Successful Spending' --Young, married couples with children and high income, living inouter-suburban/metro-fringe areas
        when experian_mosaic_type_cd = 'B06' then 'Careers & Kids' --Gen X families with children, living in expensive properties insuburban areas, with high income
        when experian_mosaic_type_cd = 'B07' then 'Fruitful Families' --Gen X families with many children, living in metro-fringe areas,with high income
        when experian_mosaic_type_cd = 'C08' then 'Rooftops & Careers' --Well-educated, high-powered business people with very high incomeand no children, living in expensive properties in central Sydney
        when experian_mosaic_type_cd = 'C09' then 'Elite Alternatives' --Well-educated professionals, living in trendy inner-urban areas,with high income
        when experian_mosaic_type_cd = 'C10' then 'Power Couples' --Young diverse couples, well-educated, transient, city centrerenters with high income and no children
        when experian_mosaic_type_cd = 'D11' then 'Scenic Connection' --Older couples in semi-retirement, living in suburban areas andnearby towns for many years, with high income
        when experian_mosaic_type_cd = 'D12' then 'Journeyed Equity' --Elderly couples from multicultural backgrounds living inexpensive properties in suburban areas of Sydney & Melbourne
        when experian_mosaic_type_cd = 'D13' then 'Coastal Comfort' --Retired, traditional couples living in coastal and scenic areas, withaverage pensionable income levels
        when experian_mosaic_type_cd = 'E14' then 'Spacious Traditions' --Middle-aged, traditional families with older children, owning largeand expensive properties in outer-suburban areas with high income
        when experian_mosaic_type_cd = 'E15' then 'Opulent Designs' --Middle-aged families owning huge houses in outer-suburbanareas, with high income
        when experian_mosaic_type_cd = 'E16' then 'Hardware & Acreage' --Working in trades, middle-aged families owning acreages of landwith large properties just outside the metro fringe
        when experian_mosaic_type_cd = 'F17' then 'Determined Suburbans' --Professional couples and singles with high income, owning theirfirst home in high growth inner suburbs
        when experian_mosaic_type_cd = 'F18' then 'Developing Domestics' --Young first-home-owner families with very young children, recentlymoved into new housing estates, with above average income
        when experian_mosaic_type_cd = 'F19' then 'Striving Scholars' --Young, highly educated singles and couples, with above averageincome, living in high growth suburbs
        when experian_mosaic_type_cd = 'G20' then 'Youthful Ambition' --Young singles and couples, some students, with no children,renting flats in inner-urban areas, with average income
        when experian_mosaic_type_cd = 'G21' then 'Emerging Metros' --Young, well-educated and culturally diverse, renting flats in suburban areas of Sydney, with above average income and no children
        when experian_mosaic_type_cd = 'G22' then 'Spirit Questers' --Millennial singles renting in coastal tourist areas, with belowaverage income
        when experian_mosaic_type_cd = 'G23' then 'Global Studies' --Young student renters near university campuses, culturallydiverse with very low or no income but high spend
        when experian_mosaic_type_cd = 'H24' then 'Backyards & Mates' --Millennial blue-collar couples and singles, living in outersuburbanareas and surrounding towns with average income
        when experian_mosaic_type_cd = 'H25' then 'Prams & Trades' --Younger blue-collar families with many children, living in newouter-suburban housing estates, with low to average income
        when experian_mosaic_type_cd = 'H26' then 'Earnest Internationals' --Younger, diverse blue-collar commuters renting apartments inSydney outer-suburban areas, with low income
        when experian_mosaic_type_cd = 'H27' then 'Township Solos' --Younger blue-collar singles in regional towns, with low incomebut have financial stability
        when experian_mosaic_type_cd = 'I28' then 'Schools & Bills' --Millennial families with young children, sometimes single parents,commuting from outer-suburban areas with average incomes
        when experian_mosaic_type_cd = 'I29' then 'Middle of the Road' --Older traditional families with older children, commuting from themetro-fringe with average to high incomes
        when experian_mosaic_type_cd = 'I30' then 'Regional Essentials' --Couples and single parents with children living in regional areaswith low to average incomes
        when experian_mosaic_type_cd = 'J31' then 'Minerals & Airports' --Mixture of singles and couples in mining towns, sometimes withchildren, earning high incomes
        when experian_mosaic_type_cd = 'J32' then 'Selfless & Hardworking' --Blue-collar families from multicultural backgrounds, living inouter-suburban areas, with average to high income
        when experian_mosaic_type_cd = 'J33' then 'Life in the Slow Lane' --Middle-aged, blue-collar couples living in outer-suburban/metrofringe areas, with average income
        when experian_mosaic_type_cd = 'J34' then 'Country Town Courage' --Low education, monocultural, manual workers with low income,living in low value properties in rural towns
        when experian_mosaic_type_cd = 'K35' then 'Mature Modernites' --Middle-aged couples without children, renting in inner suburbanapartments and terraces
        when experian_mosaic_type_cd = 'K36' then 'New-found Freedom' --Middle-aged, empty nester couples living in outer-suburban/metro-fringe areas, with above average income
        when experian_mosaic_type_cd = 'K37' then 'Realistic Horizons' --Gen X couples and sharers living in outer-suburban and regionalareas, with low income and small properties
        when experian_mosaic_type_cd = 'L38' then 'Reset Regionals' --Blue-collar families, often single parents, living in rural townswith low income and dependent children
        when experian_mosaic_type_cd = 'L39' then 'New-found Life' --Multicultural families, sometimes single parents, living in outersuburban areas with low income
        when experian_mosaic_type_cd = 'L40' then 'Satellite Battlers' --Low income singles in regional towns, sometimes living in socialhousing
        when experian_mosaic_type_cd = 'L41' then 'Downtown Blues' --Older single and diverse, city centre renters with very low income,often living in social housing
        when experian_mosaic_type_cd = 'L42' then 'Township Assistance' --Younger families, often single parents, with low incomes inregional towns, often living in social housing
        when experian_mosaic_type_cd = 'M43' then 'Blue-collar Retirees' --Multicultural older couples living in outer-suburban areas for along time, with low income but high property value
        when experian_mosaic_type_cd = 'M44' then 'Staying Put' --Older, retired couples, sometimes with adult children or carers,who are long term resident in outer-suburban areas
        when experian_mosaic_type_cd = 'M45' then 'Lonesome Elders' --Older singles, living in outer-suburban areas and satellite towns,with below average income
        when experian_mosaic_type_cd = 'M46' then 'Retirement Village' --Elderly, traditional couples and singles living in retirementvillages in cities and regional towns
        when experian_mosaic_type_cd = 'M47' then 'Rural Retirement' --Elderly couples, sometimes with adult children or carers, who arelong term residents in rural towns, with low pension income
        when experian_mosaic_type_cd = 'N48' then 'Farming Reliance' --Rural farmers and farm owners with below average income,living 10-40km away from the nearest town
        when experian_mosaic_type_cd = 'N49' then 'Outback Comfort' --Very rural farmers and farm owners with below average income,living 40km+ from the nearest town
        when experian_mosaic_type_cd = 'N50' then 'Soil & Toil' --Single farm workers in very small rural towns. with low incomeand low value properties
        when experian_mosaic_type_cd = 'N51' then 'Rustic Isolation' --Low education, traditional, singles in far inland remote towns,with low income and low value properties
    end as Mosaic_Type
    ,case
        when experian_mosaic_group_cd = 'A' then 'First Class Life' --Wealthiest group in Australia, typically older middleagedfamilies with significant assets and income
        when experian_mosaic_group_cd = 'B' then 'Comfortable Foundations' --Gen X families with school-aged children, working inwhite-collar professions and living in suburban areas
        when experian_mosaic_group_cd = 'C' then 'Striving for Status' --Young, successful, career-driven professionals living incentral city areas with high income and no children
        when experian_mosaic_group_cd = 'D' then 'Secure Tranquility' --Affluent retirees living in higher valued properties indesirable areas
        when experian_mosaic_group_cd = 'E' then 'Family Fringes' --Middle-aged traditional families living on large outersuburban plots, with comfortable incomes and longcommutes
        when experian_mosaic_group_cd = 'F' then 'Establishing Roots' --Millennial first home buyers, living 10km+ from the citycentre with above average income
        when experian_mosaic_group_cd = 'G' then 'Growing Independence' --Educated millennials at the start of their careers,renting apartments close to city centres
        when experian_mosaic_group_cd = 'H' then 'Middle Blue-collars' --Younger blue-collar workers renting far from citycentres, with below average income
        when experian_mosaic_group_cd = 'I' then 'Traditional Pursuits' --Average income traditional families & single parentswith school-aged children living in outer suburban andregional locations
        when experian_mosaic_group_cd = 'J' then 'True Grit' --Blue-collar households in gainful employment, residing inlocations across outer suburban, regional and mining towns
        when experian_mosaic_group_cd = 'K' then 'Mature Freedom' --Gen X couples without children, renting apartments andterraces in high growth suburbs
        when experian_mosaic_group_cd = 'L' then 'Hardship & Perseverance' --Unemployed and blue-collar workers living in units andflats on low incomes
        when experian_mosaic_group_cd = 'M' then 'Graceful Ageing' --Older retirees with below average income, living inowned properties or retirement villages
        when experian_mosaic_group_cd = 'N' then 'Rural Commitment' --Rural people working in agriculture, living on large plotsof land far from main roads and main towns
    end as Mosaic_Group
    ,case
        when experian_affluence_cd = 1 then 'Low income and assets'
        when experian_affluence_cd = 2 then 'Average income and low assets'
        when experian_affluence_cd = 3 then 'High income and low assets'
        when experian_affluence_cd = 4 then 'Average income and assets'
        when experian_affluence_cd = 5 then 'Low incomes and high assets'
        when experian_affluence_cd = 6 then 'High incomes and assets'
        when experian_affluence_cd = 7 then 'Highest incomes and assets'
    end as Affluence
    ,case
        when experian_household_cmpstn_cd = 1 then 'Families'
        when experian_household_cmpstn_cd = 3 then 'Couple'
        when experian_household_cmpstn_cd = 4 then 'Single Parent'
        when experian_household_cmpstn_cd = 5 then 'Single'
        when experian_household_cmpstn_cd = 6 then 'Homeshares'
    end as Household_Composition
    ,case
        when experian_household_income_cd = 1 then '<=$20,799'
        when experian_household_income_cd = 2 then '$20,800 to $41,599'
        when experian_household_income_cd = 3 then '$41,600 to $64,999'
        when experian_household_income_cd = 4 then '$65,000 to $90,999'
        when experian_household_income_cd = 5 then '$91,000 to $129,999'
        when experian_household_income_cd = 6 then '$130,000 to $181,999'
        when experian_household_income_cd = 7 then '$182,200+'
    end as Household_Income
    ,case
        when experian_head_of_household_age_cd = 1 then '18 to 19'
        when experian_head_of_household_age_cd = 2 then '20 to 24'
        when experian_head_of_household_age_cd = 3 then '25 to 29'
        when experian_head_of_household_age_cd = 4 then '30 to 34'
        when experian_head_of_household_age_cd = 5 then '35 to 39'
        when experian_head_of_household_age_cd = 6 then '40 to 44'
        when experian_head_of_household_age_cd = 7 then '45 to 49'
        when experian_head_of_household_age_cd = 8 then '50 to 54'
        when experian_head_of_household_age_cd = 9 then '55 to 59'
        when experian_head_of_household_age_cd = 10 then '60 to 64'
        when experian_head_of_household_age_cd = 11 then '65 to 69'
        when experian_head_of_household_age_cd = 12 then '70 to 74'
        when experian_head_of_household_age_cd = 13 then '75 to 79'
        when experian_head_of_household_age_cd = 14 then '80 to 84'
        when experian_head_of_household_age_cd = 15 then '85+'
    end as Household_Age
    ,case
        when experian_lifestage_cd = 1 then 'Young Families'
        when experian_lifestage_cd = 2 then 'Independent Youth'
        when experian_lifestage_cd = 3 then 'Maturing Couples and Families'
        when experian_lifestage_cd = 4 then 'Maturing Independence'
        when experian_lifestage_cd = 5 then 'Established Couples and Families'
        when experian_lifestage_cd = 6 then 'Older Couples and Families'
        when experian_lifestage_cd = 7 then 'Older Independence'
        when experian_lifestage_cd = 8 then 'Elderly Families'
        when experian_lifestage_cd = 9 then 'Elderly Couples'
        when experian_lifestage_cd = 10 then 'Elderly Singles'
    end as Experian_Lifestage
    ,case
        when experian_childn_0_10_prbblty_cd = 1 then 'Extremely Low Likelihood'
        when experian_childn_0_10_prbblty_cd = 2 then 'Very Low Likelihood'
        when experian_childn_0_10_prbblty_cd = 3 then 'Low Likelihood'
        when experian_childn_0_10_prbblty_cd = 4 then 'Below Average Likelihood'
        when experian_childn_0_10_prbblty_cd = 5 then 'Average Likelihood'
        when experian_childn_0_10_prbblty_cd = 6 then 'Above Average Likelihood'
        when experian_childn_0_10_prbblty_cd = 7 then 'Moderately High Likelihood'
        when experian_childn_0_10_prbblty_cd = 8 then 'High Likelihood'
        when experian_childn_0_10_prbblty_cd = 9 then 'Very High Likelihood'
        when experian_childn_0_10_prbblty_cd = 10 then 'Extremely High Likelihood'
    end as Children_0_10_Probability
    ,case
        when experian_childn_11_18_prbblty_cd = 1 then 'Extremely Low Likelihood'
        when experian_childn_11_18_prbblty_cd = 2 then 'Very Low Likelihood'
        when experian_childn_11_18_prbblty_cd = 3 then 'Low Likelihood'
        when experian_childn_11_18_prbblty_cd = 4 then 'Below Average Likelihood'
        when experian_childn_11_18_prbblty_cd = 5 then 'Average Likelihood'
        when experian_childn_11_18_prbblty_cd = 6 then 'Above Average Likelihood'
        when experian_childn_11_18_prbblty_cd = 7 then 'Moderately High Likelihood'
        when experian_childn_11_18_prbblty_cd = 8 then 'High Likelihood'
        when experian_childn_11_18_prbblty_cd = 9 then 'Very High Likelihood'
        when experian_childn_11_18_prbblty_cd = 10 then 'Extremely High Likelihood'
    end as Children_11_18_Probability
into [DATAMART_BIDWR_CI_dev_DATA].[d768325_HHOLD_MOSAIC]
from datamart_bidwr_smart_view.BETA_HHOLD_CANVAS
where clndr_dt = (select max(clndr_dt) from datamart_bidwr_smart_view.BETA_HHOLD_CANVAS)
;



select top 100 * from datamart_bidwr_smart_view.BETA_HHOLD_CANVAS
where clndr_dt = (select max(clndr_dt) from datamart_bidwr_smart_view.BETA_HHOLD_CANVAS)




--------------------------------------
-------TECH TYPE CHANGE HISTORY-------
--------------------------------------
IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'd768325_TechType_ChangeHistory' AND schema_id = SCHEMA_ID('DATAMART_BIDWR_CI_dev_DATA'))     DROP TABLE [DATAMART_BIDWR_CI_dev_DATA].[d768325_TechType_ChangeHistory];




-- TECH CHANGE
with dataset as (
        select
        sbl_cstr_id
        , case
            when tech_type = '4G/5G Sub 6GHz' then '5G Fixed Wireless' -- Align SFDC Naming Convention to RCRM
            when tech_type = 'NBN Fixed Wireless' then 'NBN FW' -- Consolidate Naming Conventions
            when tech_type = 'LeoStarlink' then 'Starlink'
            else tech_type
            end as tech_type
        , adbor_id
        , min(clndr_dt) as Min_clndr
        , max(clndr_dt) as Max_clndr
    from datamart_bidwr_smart_view.internet_canvas_daily
    where clndr_dt >= GetDate()-180
    group by
        sbl_cstr_id
        ,case
            when tech_type = '4G/5G Sub 6GHz' then '5G Fixed Wireless' -- Align SFDC Naming Convention to RCRM
            when tech_type = 'NBN Fixed Wireless' then 'NBN FW' -- Consolidate Naming Conventions
            when tech_type = 'LeoStarlink' then 'Starlink'
            else tech_type
            end
        , adbor_id
) SELECT
    count (distinct a.sbl_cstr_Id) as customer_count
    , a.tech_type as Prev_TechType
    , b.tech_type as New_TechType
    ,mosaic_type
    , mosaic_group
    , affluence
    , household_composition
    , household_income
    , household_age
    , experian_lifestage
    , children_0_10_probability
    , Children_11_18_probability
    ,dateadd(day, ((13 - (DATEPART(WEEKDAY,(cast(a.max_clndr as date))))) % 7), CAST(a.max_clndr AS DATE)) as TechChange_Date
into [DATAMART_BIDWR_CI_dev_DATA].[d768325_TechType_ChangeHistory]
from dataset a
inner join dataset b 
    on a.adbor_id = b.adbor_id
    and a.max_clndr between dateadd(day,-7,b.min_clndr) and dateadd(day,+7,b.min_clndr)
    and a.tech_type <> b.tech_type
left join (select ID as ADBOR_ID, GNAF_PID from datamart_bidwr_smart_view.ADDRS_keymap group by ID , GNAF_PID) ak on ak.adbor_id = a.adbor_id
LEFT JOIN [DATAMART_BIDWR_CI_dev_DATA].[d768325_HHOLD_MOSAIC] c
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
    , experian_lifestage
    , children_0_10_probability
    , Children_11_18_probability
    ,dateadd(day, ((13 - (DATEPART(WEEKDAY,(cast(a.max_clndr as date))))) % 7), CAST(a.max_clndr AS DATE)) 
;

  








-----------------------------
--------FBB REMAINING--------
-----------------------------

-- Uses Keystone Siebel Locations as the source 
IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'd768325_Fixed_Remaining' AND schema_id = SCHEMA_ID('DATAMART_BIDWR_CI_dev_DATA'))     DROP TABLE [DATAMART_BIDWR_CI_dev_DATA].[d768325_Fixed_Remaining];


with PriorityAssistance as (
    select distinct cac -- select distinct data_date
    from [DATAMART_BIDWR_SMART_VIEW].[XTRNL_DAC_DGBR_COHORT]
    where dgbr_cohort = 'DGBR006'
)
,regional_remote as (
    select distinct cac, dgbr_cohort -- select distinct data_date
    from [DATAMART_BIDWR_SMART_VIEW].[XTRNL_DAC_DGBR_COHORT]
    where dgbr_cohort in ('DGBR021' -- Regional Community
                    ,'DGBR022' -- Remote Community
                    ,'DGBR022A' -- Remote Community Hotline Case
                    ,'DGBR022B' -- Remote Community Additional Suburbs'                    
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
        , experian_lifestage
        , children_0_10_probability
        , Children_11_18_probability
    from (
        select cstmr_id
        , adbor_id,
        case
            when Plan_part_num in('XAE00001214','XAE00001114','XAE00001236') then 'FBB Starter' -- $65
            when Plan_part_num in('XAE00001237','XAE00001024','XAE00001215','XAE00001050','XAE00001190','XAE00001123'
                    ,'XAE00001019','XAE00001230','XAE00001217') then 'FBB Basic'        -- $85
            when Plan_part_num in('XAE00001216','XAE00001051','XAE00001026','XAE00001116'
                                ,'XAE00001124','XAE00001189','XAE00001241') then 'FBB Essential'    -- $100
            when Plan_part_num = 'XAE00001232' THEN
                        CASE
                            when Speed_tier in ('Superfast','Superfast Speed') then 'FBB Superfast'
                            when Speed_tier = 'Ultrafast' then 'FBB Ultrafast'    --$180
                            else 'FBB Premium'
                        end
            else "PLAN"
            end as plan_name
        ,'ADSL - Internet' as tech_type
        , fbb_service_id as ServiceID -- select distinct tech_type
    from DATALAB_BIDWR_SMART_DATA.keystone_fbb
        where tech_type = 'ADSL'
        UNION
    select distinct cstmr_id, adbor_id, "plan", 'PSTN - Voice' as tech_type, voice_service_id -- select distinct tech_type
    from DATALAB_BIDWR_SMART_DATA.keystone_voice
        where voice_standalone = 'Yes'
        and tech_type = 'ADSL'
    ) l
left join DATAMART_BIDWR_SMART_VIEW.NBN_FOOTPRINT rfs on rfs.adbor_id = l.adbor_id
left join (select ID as ADBOR_ID, GNAF_PID from datamart_bidwr_smart_view.ADDRS_keymap group by ID , GNAF_PID) ak on ak.adbor_id = RFS.adbor_id
LEFT JOIN [DATAMART_BIDWR_CI_dev_DATA].[d768325_HHOLD_MOSAIC] c
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
        , experian_lifestage
        , children_0_10_probability
        , Children_11_18_probability
    into [DATAMART_BIDWR_CI_dev_DATA].[d768325_Fixed_Remaining]
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
        , experian_lifestage
        , children_0_10_probability
        , Children_11_18_probability







-----------------------------
----------FBB USAGE----------
-----------------------------

-- Step 1 Customers -- ~30mins
-- Pulls data from Account Canvas CSTMR:ADBOR:Service Relationship
IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'd768325_FBB_Usage_Customer_Services' AND schema_id = SCHEMA_ID('DATAMART_BIDWR_CI_dev_DATA'))     DROP TABLE [DATAMART_BIDWR_CI_dev_DATA].[d768325_FBB_Usage_Customer_Services];

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
    , experian_lifestage
    , children_0_10_probability
    , Children_11_18_probability
    ,eomonth(ac.clndr_dt) as Month_Value
into [DATAMART_BIDWR_CI_dev_DATA].[d768325_FBB_Usage_Customer_Services]
from [DATAMART_BIDWR_SMART_VIEW].[accnt_CANVAS_DAILY] ac
-- Check if Customer is in NBN or Non-NBN Area
left join datamart_bidwr_smart_view.nbn_footprint nf on nf.adbor_id = ac.addrs_adbor_id
-- Join Services
-- Exclude Customers from Account Canvas, where no mobile/mbb/internet services
inner join ( -- Mobile / MBB
            select sbl_cstr_id, srvc_srl_num, case when srvc_prdct_name = 'Data Service' then 'MBB' else 'Mobile' end as Product_Type, eomonth(clndr_dt) as Month_Value
            from DATAMART_BIDWR_SMART_VIEW.mobile_canvas_daily
            where clndr_dt >= dateadd(month,-6,GetDate()) -- Last 6 months of Data
            group by sbl_cstr_id, srvc_srl_num, case when srvc_prdct_name = 'Data Service' then 'MBB' else 'Mobile' end, eomonth(clndr_dt)
            UNION
            -- Internet
            select sbl_cstr_id, srvc_srl_num, 'Internet' as Product_Type, eomonth(clndr_dt) as Month_Value
            from DATAMART_BIDWR_SMART_VIEW.internet_canvas_daily
            where  clndr_dt >= dateadd(month,-6,GetDate()) -- Last 6 months of Data
            group by sbl_cstr_id, srvc_srl_num, eomonth(clndr_dt)
    ) s on s.sbl_cstr_id = ac.sbl_cstr_Id
    and s.month_value = eomonth(ac.clndr_dt)
left join (select ID as ADBOR_ID, GNAF_PID from datamart_bidwr_smart_view.ADDRS_keymap group by ID , GNAF_PID) ak 
    on ak.adbor_id = AC.ADDRS_ADBOR_ID
LEFT JOIN [DATAMART_BIDWR_CI_dev_DATA].[d768325_HHOLD_MOSAIC] c
    on ak.gnaf_pid = c.gnaf_pid

where ac.clndr_dt >= dateadd(month,-6,GetDate()) -- Last 6 months of Data
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
    , experian_lifestage
    , children_0_10_probability
    , Children_11_18_probability
    ,eomonth(ac.clndr_dt);







-- Step 2 Service Usage -- 5mins
-- Pulls Data from Usage Canvas to Establish Service:Usage Relationship
IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'd768325_FBB_Data_Usage' AND schema_id = SCHEMA_ID('DATAMART_BIDWR_CI_dev_DATA'))     DROP TABLE [DATAMART_BIDWR_CI_dev_DATA].[d768325_FBB_Data_Usage];

select srvc_srl_num
  ,data_mb_total_vlm
  ,datename(month,clndr_dt) as Month_Name
  ,clndr_dt as Month_Date
into [DATAMART_BIDWR_CI_dev_DATA].[d768325_FBB_Data_Usage]
from datamart_bidwr_smart_view.usage_canvas
where clndr_dt >= dateadd(month,-6,GetDate()) -- Last 6 months of Data
and data_mb_total_vlm > 0;




----------------
-- Final Step -- 
----------------
IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'd768325_FBB_Usage' AND schema_id = SCHEMA_ID('DATAMART_BIDWR_CI_dev_DATA'))     DROP TABLE [DATAMART_BIDWR_CI_dev_DATA].[d768325_FBB_Usage];

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
    , experian_lifestage
    , children_0_10_probability
    , Children_11_18_probability
    , NBN_Check
    , month_name
    , month_date
-- into #final_Usage
into [DATAMART_BIDWR_CI_dev_DATA].[d768325_FBB_Usage]
from (
  select
      addrs_adbor_id
      ,count(distinct c.srvc_srl_num) as SIO_Count
      -- Product Type
      , max(case when Product_Type = 'MBB' then 'MBB' end) as MBB
      , max(case when Product_Type = 'Internet' then 'Internet' end) as Internet
      , max(case when Product_Type = 'Mobile' then 'Mobile' end) as Mobile
      -- Product Usage
      , sum(case when product_type = 'MBB' then data_mb_total_vlm else 0 end)/1024 as MBB_Usage
      , sum(case when product_type = 'Internet' then data_mb_total_vlm else 0 end)/1024 as Internet_Usage
      , sum(case when product_type = 'Mobile' then data_mb_total_vlm else 0 end)/1024 as Mobile_Usage
      -- Total Usage
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
    , experian_lifestage
    , children_0_10_probability
    , Children_11_18_probability
  from [DATAMART_BIDWR_CI_dev_DATA].[d768325_FBB_Usage_Customer_Services] c
  left join [DATAMART_BIDWR_CI_dev_DATA].[d768325_FBB_Data_Usage] d on d.srvc_srl_num = c.srvc_srl_num
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
    , experian_lifestage
    , children_0_10_probability
    , Children_11_18_probability
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
    , experian_lifestage
    , children_0_10_probability
    , Children_11_18_probability

















--------------------------------
----ADDRESS PRODUCT HOLDINGS----
--------------------------------
IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'd768325_Address_Holdings_S1' AND schema_id = SCHEMA_ID('DATAMART_BIDWR_CI_dev_DATA'))     DROP TABLE DATAMART_BIDWR_CI_dev_DATA.d768325_Address_Holdings_s1;


-- Uses Account canvas as the primary source for all Service Combinations per address
with internet_canvas as (
 select
        a.sbl_cstr_id
        ,a.srvc_srl_num
        ,a.Plan_Prdct_Cd
        ,a.plan_name
        ,CASE WHEN a.adbor_id is not null then a.tech_type else null end as service_tech_Type
      --  ,a.tech_type as service_tech_Type
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
                        when Speed_Prdct_Name = 'Ultrafast' then 'FBB Ultrafast'    --$180
                        else 'FBB Premium'
                    end
    when Plan_Prdct_Cd = 'FSUBBDL-BXLG' then 'FBB Superfast'    -- $140
    when plan_prdct_cd = 'FSUBBDL-BXXL' then 'FBB Ultrafast'
    WHEN b.plan_name_group ='Internet Plan Premium' then 'FBB Premium'
    WHEN b.plan_name_group ='Home Phone Plan'  THEN 'FBB HomePhone'
WHEN b.plan_name_group in ('Out of Market Plan','Foxtel Bundle','Internet Trial Plan','Small Business Bundle','Bigpond Data plan') THEN 'Internet_Other'
ELSE 'Internet_Other'
    end as Plan_name_parent
  --  INTO #INTERNET_CANVAS
from datamart_bidwr_smart_view.internet_canvas_daily a
LEFT JOIN [DATAMART_BIDWR_CI_prd_DATA].D768325_FIXED_PLAN_GROUPS b
on a.plan_name = b.plan_name
left join (select ID as ADBOR_ID, GNAF_PID from datamart_bidwr_smart_view.ADDRS_keymap group by ID , GNAF_PID) ak on ak.adbor_id = a.adbor_id
where clndr_dt = (select max(clndr_dt) from datamart_bidwr_smart_view.internet_canvas_daily)
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
                        when Speed_Prdct_Name = 'Ultrafast' then 'FBB Ultrafast'    --$180
                        else 'FBB Premium'
                    end
    when Plan_Prdct_Cd = 'FSUBBDL-BXLG' then 'FBB Superfast'    -- $140
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
            --when Plan_Prdct_Cd in ('VMP0000201','VMP0000213','MSUBVOD-VXLG') then 'DV HH XL'              
            WHEN Plan_Prdct_Cd IN('VMP0000227','MSUBVOD-VCMP') THEN 'DV HH Bdl'            
            when Plan_Prdct_Cd in ('VMP0000196','MSUBDAT-DXSM') then 'DV MBB XS'                
            when Plan_Prdct_Cd in ('VMP0000197','MSUBDAT-DSML') then 'DV MBB S'            
            when Plan_Prdct_Cd in ('VMP0000198','VMP0000221','MSUBDAT-DMED','MSUBDAT-DMED-V02') then 'DV MBB M'            
            when Plan_Prdct_Cd in ('VMP0000200','VMP0000222','MSUBDAT-DLRG','MSUBDAT-DLRG-V02') then 'DV MBB L'            
            WHEN Plan_Prdct_Cd IN('VMP0000228','MSUBDAT-DCMP') THEN 'DV MBB Bdl'       
            WHEN Srvc_Prdct_Sbtyp = 'Data' THEN 'MBB_Other'

            else 'Mobile_Other'            
          end as Plan_name_parent
    
    -- INTO #MOBILE_CANVAS
    from [DATAMART_BIDWR_SMART_VIEW].[MOBILE_CANVAS_DAILY] m          
          where clndr_dt in (Select Max(clndr_dt) from [DATAMART_BIDWR_SMART_VIEW].[MOBILE_CANVAS_DAILY] )  
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
--INTO #PREPAID_CANVAS            
    from [DATAMART_BIDWR_SMART_VIEW].[PREPAID_CANVAS_daily]            
        where clndr_dt = (select max(Clndr_Dt) from [DATAMART_BIDWR_SMART_VIEW].[PREPAID_CANVAS_daily])            
        and BU_Cd in('C','B')
)

,fbb_canvas as (
    select
        GNAF_PID
        ,MAX(case when ic2.srvc_cnnct_dt >=GetDate() -90 then 'New to Telstra (>= 90 days)' end) as New_Active
        ,MAX(case when ic2.adbor_id is not null then 'Active Telstra' end) as Current_Active
        ,MAX(case when ic2.adbor_id is null then 'Left Telstra (<= 90 days)' end) as Previously_Active
--INTO #FBB_CANVAS  
    from datamart_bidwr_smart_view.internet_canvas_daily ic1
    -- LEFT JOIN for GNAF_PID
    left join (select  ID as ADBOR_ID, GNAF_PID from datamart_bidwr_smart_view.ADDRS_keymap group by ID, GNAF_PID) ak on ak.adbor_id = ic1.adbor_id
    -- Left Join Current UpToDate Information about each location
    left join (
            select distinct
                ic2.adbor_id
                ,srvc_cnnct_dt
            from datamart_bidwr_smart_view.internet_canvas_daily ic2
            where clndr_dt = (select max(clndr_dt) from datamart_bidwr_smart_view.internet_canvas_daily)
            and BU_CD in ('C','B')
        ) ic2 on ic1.adbor_id = ic2.adbor_id
    where clndr_dt >=GetDate() -90
    and BU_CD in ('C','B')
    group by
        GNAF_PID
)

--UNIONING FIXED / MOBILE / PREPAID SERVICES FOR ADBOR REPORTING 
, 
service_data_s1 as (
select sbl_Cstr_id, srvc_srl_num,bu_cd, plan_name,service_tech_type, adbor_id, plan_name_parent, null as plan_type_Cat, 'Internet_Canvas' as Source from internet_canvas
UNION ALL
select sbl_cstr_id, srvc_srl_num, bu_cd, null, null, null, plan_name_parent, null, 'PrePaid_Canvas'  from prepaid_canvas
UNION ALL
select sbl_Cstr_id, srvc_srl_num, bu_cd, null, null, null,plan_name_parent, plan_type_Cat, 'Mobile_Canvas' from mobile_canvas
)

, service_tech as (
    select distinct service_tech_type, adbor_id from service_data_s1 )        --JOINING TO GET FIXED_SERVICE_TECH_TYPE

,service_data as(
select 
ak.GNAF_PID 
--, ak.adbor_id
,  CASE WHEN ak.GNAF_PID IS NOT NULL THEN string_Agg(c.service_tech_type, ', ') WITHIN GROUP (order by c.service_tech_type) ELSE NULL END as fixed_service_tech_type
--,  CASE WHEN ak.GNAF_PID IS NOT NULL THEN max(c.service_tech_type) ELSE NULL END as fixed_service_tech_type

        ,MAX(case when a.source = 'Internet_Canvas' then 'Internet' end) as Internet 
        ,MAX(case when a.source = 'Mobile_Canvas' then 'Mobile' end) as Mobile
        ,MAX(case when a.source = 'PrePaid_Canvas' then 'PrePaid' end) as PrePaid
        ,MAX(case when a.Plan_Type_Cat = 'Data' then 'MBB' end) as MBB
        --,MAX(case when fft_ind = 1 then 'Foxtel' end) as Foxtel
        ,COUNT(DISTINCT case when a.bu_cd = 'C' then a.sbl_Cstr_id else null end) as Consumer_Count         --Consumer Customer Count
        ,COUNT(DISTINCT case when a.bu_cd = 'B' then a.sbl_Cstr_id else null end) as Business_Count         -- BUSINESS CUSTOMER COUNT
        ,COUNT(DISTINCT case when a.bu_cd = 'C' then a.srvc_srl_num else null end) as Consumer_Services         --Consumer services
        ,COUNT(DISTINCT case when a.bu_cd = 'B' then a.srvc_srl_num else null end) as Business_Services         --Business services
           
        -- Fixed Internet Services
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
        -- PrePaid
        ,sum(case when a.plan_name_parent = 'PrepTelstra_HH' then 1 else 0 end) as PrepTelstra_HH          
        ,sum(case when a.plan_name_parent = 'PrepTelstra_MBB' then 1 else 0 end) as PrepTelstra_MBB            
        ,sum(case when a.plan_name_parent = 'PrepBoost_HH' then 1 else 0 end) as PrepBoost_HH
        ,sum(case when a.plan_name_parent = 'Prepaid Other' then 1 else 0 end) as Prep_Other                   
        -- Mobile Canvas
        ,sum(case when a.plan_name_parent = 'DV HH Starter' then 1 else 0 end) as Mobile_DVHHS
        ,sum(case when a.plan_name_parent = 'DV HH Basic' then 1 else 0 end) as Mobile_DVHHB              
        ,sum(case when a.plan_name_parent = 'DV HH Essential' then 1 else 0 end) as Mobile_DVHHE              
        ,sum(case when a.plan_name_parent = 'DV HH Premium' then 1 else 0 end) as Mobile_DVHHP            
        ,sum(case when a.plan_name_parent = 'DV HH Bdl' then 1 else 0 end) as Mobile_DVHHBdl      
        ,sum(case when a.plan_name_parent = 'Mobile_Other' then 1 else 0 end) as Mobile_Other                 
        -- Mobile Broadband                
        ,sum(case when a.plan_name_parent = 'DV MBB XS' then 1 else 0 end) as Mobile_DVMBBXS              
        ,sum(case when a.plan_name_parent = 'DV MBB S' then 1 else 0 end) as Mobile_DVMBBS            
        ,sum(case when a.plan_name_parent = 'DV MBB M' then 1 else 0 end) as Mobile_DVMBBM            
        ,sum(case when a.plan_name_parent = 'DV MBB L' then 1 else 0 end) as Mobile_DVMBBL            
        ,sum(case when a.plan_name_parent = 'DV MBB Bdl' then 1 else 0 end) as Mobile_DVMBBBdl
        ,sum(case when a.plan_name_parent = 'MBB_Other' then 1 else 0 end) as MBB_Other     
--INTO #service_data 
from service_data_s1 a
LEFT join (select sbl_Cstr_id, addrs_adbor_id, BU_CD from [DATAMART_BIDWR_SMART_VIEW].[accnt_CANVAS_DAILY]  where clndr_dt in (Select Max(clndr_dt) from [DATAMART_BIDWR_SMART_VIEW].[accnt_CANVAS_DAILY] ) ) b
    on a.sbl_Cstr_id = b.sbl_cstr_id
LEFT join (select distinct ID as ADBOR_ID, GNAF_PID from datamart_bidwr_smart_view.ADDRS_keymap) ak on coalesce(a.adbor_id,b.addrs_adbor_id) = ak.adbor_id      --FOR INTERNET SERVICES WE'RE GETTING THE ADBOR FROM THE INTERNET CANVAS, THE REMAINING ARE FROM THE ACCOUNT CANVAS
--where b.BU_CD ='C'
LEFT JOIN service_tech c 
on a.adbor_id = c.adbor_id
group by
ak.GNAF_PID 
--, ak.adbor_id
)



,household_canvas as (
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
    ,mb_category as MeshBlock_Category
    -- Internet Plans
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
    -- Prepaid
    ,PrepTelstra_HH
    ,PrepTelstra_MBB
    ,PrepBoost_HH
    , Prep_Other               
    -- Mobiles
    ,Mobile_DVHHS
    ,Mobile_DVHHB
    ,Mobile_DVHHE
    ,Mobile_DVHHP
    ,Mobile_DVHHBdl
    ,Mobile_Other               
    -- Mobile Broadband
    ,Mobile_DVMBBXS
    ,Mobile_DVMBBS
    ,Mobile_DVMBBM
    ,Mobile_DVMBBL
    ,Mobile_DVMBBBdl
    ,MBB_Other                  
    -- Total Count of SIOs
-- Mobiles PostPaid Services
    ,   Mobile_DVHHS -- HH Starter
        + Mobile_DVHHB -- HH Basic
        + Mobile_DVHHE -- HH Essential
        + Mobile_DVHHP -- HH Premium
        + Mobile_DVHHBdl -- HH Bundle
        + Mobile_Other   -- HH Other
-- MBB services
        + Mobile_DVMBBXS -- MBB Xsmall
        + Mobile_DVMBBS -- MBB Small
        + Mobile_DVMBBM -- MBB Medium
        + Mobile_DVMBBL -- MBB Large
        + Mobile_DVMBBBdl -- MBB Bundle
        + MBB_Other                  
 -- Fixed Internet Services 
        + Internet_DVS      -- FBB Starter
        + Internet_DVB      -- FBB Basic
        + Internet_DVE      -- FBB Essential
        + Internet_DVP      -- FBB Premium
        + Internet_DVU      -- FBB Ultimate / Superfast
        + Internet_DVUplus  -- FBB Ultimate+ / Ultrafast
        + Internet_fiveg    --FBB 5g
        + Internet_strlnk   --FBB Starlink            
        + Internet_Other    --FBBRemaining groups           
        + Internet_homep    -- Salesforce Homephone 
 -- Prepaid        
        + PrepTelstra_HH
        + PrepTelstra_MBB
        + PrepBoost_HH
        + Prep_Other 
        as SIOs_per_Household
    ,   

--Fixed Sios per household
        Internet_DVS -- FBB Starter
        + Internet_DVB -- FBB Basic
        + Internet_DVE -- FBB Essential
        + Internet_DVP -- FBB Premium
        + Internet_DVU -- FBB Ultimate / Superfast
        + Internet_DVUplus -- FBB Ultimate+ / Ultrafast
        + Internet_fiveg
        + Internet_strlnk           
        + Internet_Other             
        + Internet_homep
        as Fixed_SIOs_per_Household 

--Mobile SIOS PER HOUSEHOLD    
    ,   Mobile_DVHHS -- HH Starter
        + Mobile_DVHHB -- HH Basic
        + Mobile_DVHHE -- HH Essential
        + Mobile_DVHHP -- HH Premium
        + Mobile_DVHHBdl -- HH Bundle
        -- Mobiles PostPaid Services
        + Mobile_DVMBBXS -- MBB Xsmall
        + Mobile_DVMBBS -- MBB Small
        + Mobile_DVMBBM -- MBB Medium
        + Mobile_DVMBBL -- MBB Large
        + Mobile_DVMBBBdl -- MBB Bundle
        + MBB_Other                 
        + Mobile_Other   -- HH Other
        as MObile_SIOs_per_Household

   --PREPAID PER HOUSEHOLD     
    ,   PrepTelstra_HH
        + PrepTelstra_MBB
        + PrepBoost_HH
        + Prep_Other 
        as Prepaid_SIOs_per_Household 
        ,case
            when New_Active is not null then New_Active -- Prioritise New Activations to be visible on the dashboard over
            when Current_Active is not null then Current_Active -- Existing Telstra Active FBB Services
            when Previously_Active is not null then Previously_Active
            when Internet_DVS + Internet_DVB + Internet_DVE + Internet_DVP + Internet_DVU + Internet_DVUplus + Internet_fiveg + Internet_strlnk + Internet_Other >0 then 'Active Telstra'  
            else 'Not with Telstra'
        end as FBB_Status
--Drop table #household_canvas
from datamart_bidwr_smart_view.beta_hhold_canvas hh
-- Join FBB Status
left join fbb_canvas FS on fs.gnaf_pid = hh.gnaf_pid
-- Join Account Canvas
left join service_data sd on sd.GNAF_PID = hh.GNAF_PID
-- Join for Premises Category
left join (select distinct gnaf_pid, mb_category from [DATAMART_BIDWR_SMART_VIEW].addrs_gnaf) ag on ag.gnaf_pid = hh.gnaf_pid
where clndr_dt = (select max(clndr_dt) from datamart_bidwr_smart_view.beta_hhold_canvas)
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
                -- FTTC
                when srvc_class_desc = 'Serviceable by FTTC, drop in place, DPU cut in complete, service had been activated' then 'NBN FTTC'
                when srvc_class_desc = 'Serviceable by FTTC, drop in place, but cut in at DPU is required' then 'NBN FTTC - Not Installed'
                when srvc_class_desc = 'Serviceable by FTTC, drop in place, DPU cut in complete, service had not been activated yet' then 'NBN FTTC - Not Installed'
                when srvc_class_desc = 'Serviceable by FTTC but there is no copper path / lead in yet available' then 'NBN FTTC - Not Installed'
                -- FTTN
                when tech_type = 'FIBRE TO THE NODE' and srvc_class_desc = 'Serviceable by Copper, Existing Copper Pair in-place not active with NBN Co.' then 'NBN FTTN - Not Installed'
                when tech_type = 'FIBRE TO THE NODE' and srvc_class_desc = 'Serviceable by Copper, no existing Copper Pair in-place, lead-in required' then 'NBN FTTN - Not Installed'
                when tech_type = 'FIBRE TO THE NODE' and srvc_class_desc = 'Serviceable by Copper, Existing Copper Pair in-place active with NBN Co.' then 'NBN FTTN'

                -- FTTB
                when tech_type = 'FIBRE TO THE BUILDING' and srvc_class_desc = 'Serviceable by Copper, Existing Copper Pair in-place not active with NBN Co.' then 'NBN FTTB - Not Installed'
                when tech_type = 'FIBRE TO THE BUILDING' and srvc_class_desc = 'Serviceable by Copper, no existing Copper Pair in-place, lead-in required' then 'NBN FTTB - Not Installed'
                when tech_type = 'FIBRE TO THE BUILDING' and srvc_class_desc = 'Serviceable by Copper, Existing Copper Pair in-place active with NBN Co.' then 'NBN FTTB'

                -- FTTP
                when srvc_class_desc = 'Serviceable by fibre, Drop and NTD in place' then 'NBN FTTP'
                when srvc_class_desc = 'Serviceable by fibre, no Drop in place, no NTD' then 'NBN FTTP - Not Installed'
                when srvc_class_desc = 'Serviceable by fibre, Drop in place, no NTD' then 'NBN FTTP - Not Installed'
                -- FW
                when srvc_class_desc = 'Serviceable by Wireless, NTD installed' then 'NBN FW'
                when srvc_class_desc = 'Serviceable by Wireless, NTD not installed' then 'NBN FW - Not Installed'
                -- HFC
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
    -- Mosaic Attributes
    ,Mosaic_Type
    ,Mosaic_Group
    ,Affluence
    ,Household_Composition
    ,Household_Income
    ,Household_Age
    ,Experian_Lifestage
    ,Children_0_10_Probability
    ,Children_11_18_Probability
    -- Internet Plans
    ,Internet_DVS as FBB_Starter
    ,Internet_DVB as FBB_Basic
    ,Internet_DVE as FBB_Essential
    --,Internet_DVE_Bus
    --,Internet_DVP_Bus
    ,Internet_DVP as FBB_Premium
    ,Internet_DVU as FBB_Superfast
    ,Internet_DVUplus
    ,Internet_fiveg
    ,Internet_homep
    ,Internet_strlnk
    ,Internet_Other
    -- Prepaid
    ,PrepTelstra_HH
    ,PrepTelstra_MBB
    ,PrepBoost_HH
    ,Prep_Other
    -- Mobiles
    ,Mobile_DVHHS
    ,Mobile_DVHHB
    ,Mobile_DVHHE
    ,Mobile_DVHHP
    ,Mobile_DVHHBdl
    ,Mobile_Other
    -- Mobile Broadband
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
INTO [DATAMART_BIDWR_CI_dev_DATA].[d768325_Address_Holdings_S1] 
from household_canvas hh
-- Join for NBN Attributes
left join (select distinct gnaf_id, srvc_class_desc, coat_rsn, tech_type from datamart_bidwr_smart_view.nbn_footprint) nf on nf.GNAF_Id = hh.GNAF_PID
-- Join for Mosaic Attributes
left join [DATAMART_BIDWR_CI_dev_DATA].[d768325_HHOLD_MOSAIC] m on m.gnaf_pid = hh.gnaf_pid
;







------------------------------------
--------ADDING CLOSEST STORE--------
------------------------------------
IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'd768325_Address_Holdings' AND schema_id = SCHEMA_ID('DATAMART_BIDWR_CI_dev_DATA'))     DROP TABLE DATAMART_BIDWR_CI_dev_DATA.d768325_Address_Holdings;

WITH Store_Record AS (
    SELECT
        primary_dealer_code
        ,TRR_Store_name
        ,city
        ,Post_code
        ,[State]
        ,CAST(LONG AS DECIMAL(20,17)) as store_long
        ,CAST(LAT AS DECIMAL(20,17)) as store_lat
		,CAST(LONG AS DECIMAL(20,17)) - 0.8 AS longitude_offset_L
		,CAST(LONG AS DECIMAL(20,17)) + 0.8 AS longitude_offset_H
		,CAST(LAT AS DECIMAL(20,17)) - 0.8  AS latitude_offset_L
		,CAST(LAT AS DECIMAL(20,17)) + 0.8  AS latitude_offset_H
    FROM
        [DATAMART_BIDWR_CI_DEV_DATA].[d981967_TRS_RETAIL] 
)


, all_under80 AS (
SELECT
    acc.gnaf_pid,
    acc.city,
    acc.[state],
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
				POWER(SIN(RADIANS((CAST(s.store_lat AS DOUBLE PRECISION) - CAST(acc.lttd AS DOUBLE PRECISION)) / 2)), 2)
				+ COS(RADIANS(CAST(acc.lttd AS DOUBLE PRECISION))) * COS(RADIANS(CAST(s.store_lat AS DOUBLE PRECISION)))
				* POWER(SIN(RADIANS((CAST(s.store_long AS DOUBLE PRECISION) - CAST(acc.lngtd AS DOUBLE PRECISION)) / 2)), 2)
			)
		),
		2 -- Specify the number of decimal places (in this case, 2)
	) AS distance_km
FROM [DATAMART_BIDWR_CI_dev_DATA].[d768325_Address_Holdings_s1] acc
JOIN Store_Record s
    ON 1=1
WHERE acc.lngtd BETWEEN s.longitude_offset_L AND s.longitude_offset_H
AND acc.lttd BETWEEN s.latitude_offset_L AND s.latitude_offset_H
AND
	ROUND(
		6371 * 2 * ASIN(
			SQRT(
				POWER(SIN(RADIANS((CAST(s.store_lat AS DOUBLE PRECISION) - CAST(acc.lttd AS DOUBLE PRECISION)) / 2)), 2)
				+ COS(RADIANS(CAST(acc.lttd AS DOUBLE PRECISION))) * COS(RADIANS(CAST(s.store_lat AS DOUBLE PRECISION)))
				* POWER(SIN(RADIANS((CAST(s.store_long AS DOUBLE PRECISION) - CAST(acc.lngtd AS DOUBLE PRECISION)) / 2)), 2)
			)
		),
		2 -- Specify the number of decimal places (in this case, 2)
	) <= 80
) 

select 
	a.*
	,coalesce(b.TRR_Store_name, 'No stores within 80km') as TRR_Store_name
    ,coalesce(b.primary_dealer_code,'NA') as primary_dealer_code
	,b.Flag20KmDistance
INTO [DATAMART_BIDWR_CI_dev_DATA].[d768325_Address_Holdings] 
FROM [DATAMART_BIDWR_CI_dev_DATA].[d768325_Address_Holdings_S1] a
LEFT JOIN (
	SELECT distinct
		gnaf_pid,
		city,
		[state],
		pstcd,
		TRR_Store_name,
		primary_dealer_code,
		distance_km,
		CASE WHEN distance_km <= 20.00 THEN 1 ELSE 0 END AS Flag20KmDistance
		,ROW_NUMBER() OVER(PARTITION BY gnaf_pid order by distance_km asc) as rownum
	FROM all_under80
) b
ON a.GNAF_PID = b.GNAF_PID
AND b.rownum =1
--16,486,194