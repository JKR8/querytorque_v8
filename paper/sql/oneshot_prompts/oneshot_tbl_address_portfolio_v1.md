You are a senior query optimization architect. Your job is to deeply analyze a SQL query, determine the single best optimization strategy, and then produce the optimized SQL directly.

You have all the data: EXPLAIN plans, DAG costs, full constraint list, global knowledge, and the complete example catalog. Analyze thoroughly, then implement the best strategy as working SQL.

## Query: tbl_address_portfolio_v1
## Dialect: duckdb

```sql
1 | WITH broadband_canvas AS (SELECT a.customer_id, a.service_id, a.product_code, a.product_name, CASE WHEN NOT a.location_id IS NULL THEN a.technology_type ELSE NULL END AS service_technology, a.location_id, ak.address_id, a.customer_type, CASE WHEN b.plan_group = 'Plan_Group_Standard' THEN 'Plan_Tier_Standard' WHEN b.plan_group = 'Plan_Group_Basic' THEN 'Plan_Tier_Basic' WHEN b.plan_group = 'Plan_Group_Advanced' THEN 'Plan_Tier_Advanced' WHEN b.plan_group = 'Plan_Group_Premium' THEN 'Plan_Tier_Premium' WHEN b.plan_group = 'Plan_Group_High_Speed' THEN 'Plan_Tier_High_Speed' WHEN product_code = 'Plan_020' THEN CASE WHEN speed_class IN ('Speed_High', 'Speed_High_Plus') THEN 'Plan_Tier_Ultra' WHEN speed_class = 'Speed_Very_High' THEN 'Plan_Tier_Premium' ELSE 'Plan_Tier_Elite' END WHEN product_code = 'Plan_Code_001' THEN 'Plan_Tier_Ultra' WHEN product_code = 'Plan_Code_002' THEN 'Plan_Tier_Premium' WHEN b.plan_group = 'Plan_Group_Elite' THEN 'Plan_Tier_Elite' WHEN b.plan_group = 'Plan_Group_Standard_Voice' THEN 'Plan_Tier_Voice' WHEN b.plan_group IN ('Plan_Group_Bundled', 'Plan_Group_Family', 'Plan_Group_Trial', 'Plan_Group_Business', 'Plan_Group_Data') THEN 'Plan_Tier_Other' ELSE 'Plan_Tier_Other' END AS plan_parent FROM broadband_service_daily AS a LEFT JOIN plan_mapping AS b ON a.product_name = b.product_name LEFT JOIN (SELECT id AS location_id, address_id FROM address_mapping GROUP BY id, address_id) AS ak ON ak.location_id = a.location_id WHERE calendar_date = (SELECT MAX(calendar_date) FROM broadband_service_daily) AND customer_type IN ('Business', 'Consumer') AND NOT ak.address_id IS NULL GROUP BY a.customer_id, a.service_id, a.product_code, a.product_name, a.technology_type, a.location_id, ak.address_id, a.customer_type, CASE WHEN b.plan_group = 'Plan_Group_Standard' THEN 'Plan_Tier_Standard' WHEN b.plan_group = 'Plan_Group_Basic' THEN 'Plan_Tier_Basic' WHEN b.plan_group = 'Plan_Group_Advanced' THEN 'Plan_Tier_Advanced' WHEN b.plan_group = 'Plan_Group_Premium' THEN 'Plan_Tier_Premium' WHEN b.plan_group = 'Plan_Group_High_Speed' THEN 'Plan_Tier_High_Speed' WHEN product_code = 'Plan_020' THEN CASE WHEN speed_class IN ('Speed_High', 'Speed_High_Plus') THEN 'Plan_Tier_Ultra' WHEN speed_class = 'Speed_Very_High' THEN 'Plan_Tier_Premium' ELSE 'Plan_Tier_Elite' END WHEN product_code = 'Plan_Code_001' THEN 'Plan_Tier_Ultra' WHEN product_code = 'Plan_Code_002' THEN 'Plan_Tier_Premium' WHEN b.plan_group = 'Plan_Group_Elite' THEN 'Plan_Tier_Elite' WHEN b.plan_group = 'Plan_Group_Standard_Voice' THEN 'Plan_Tier_Voice' WHEN b.plan_group IN ('Plan_Group_Bundled', 'Plan_Group_Family', 'Plan_Group_Trial', 'Plan_Group_Business', 'Plan_Group_Data') THEN 'Plan_Tier_Other' ELSE 'Plan_Tier_Other' END), mobile_canvas AS (SELECT customer_id, service_id, customer_type, CASE WHEN service_category = 'Data' THEN 'Mobile_Broadband' ELSE 'Mobile_Voice' END AS plan_type_category, CASE WHEN product_code IN ('Product_001', 'Product_002', 'Product_003', 'Product_004', 'Product_005', 'Product_006') THEN 'Plan_Tier_Starter' WHEN product_code IN ('Product_007', 'Product_008', 'Product_009', 'Product_010', 'Product_011', 'Product_012') THEN 'Plan_Tier_Basic' WHEN product_code IN ('Product_013', 'Product_014', 'Product_015', 'Product_016', 'Product_017', 'Product_018') THEN 'Plan_Tier_Advanced' WHEN product_code IN ('Product_019', 'Product_020', 'Product_021', 'Product_022', 'Product_023', 'Product_024') THEN 'Plan_Tier_Premium' WHEN product_code IN ('Product_025', 'Product_026') THEN 'Plan_Tier_Bundle' WHEN product_code IN ('Product_027', 'Product_028') THEN 'Plan_Tier_Extra_Small' WHEN product_code IN ('Product_029', 'Product_030') THEN 'Plan_Tier_Small' WHEN product_code IN ('Product_031', 'Product_032', 'Product_033', 'Product_034') THEN 'Plan_Tier_Medium' WHEN product_code IN ('Product_035', 'Product_036', 'Product_037', 'Product_038') THEN 'Plan_Tier_Large' WHEN product_code IN ('Product_039', 'Product_040') THEN 'Plan_Tier_Extra_Large' WHEN service_category = 'Data' THEN 'Plan_Tier_Mobile_Data' ELSE 'Plan_Tier_Mobile_Other' END AS plan_parent FROM mobile_service_daily AS m WHERE calendar_date IN (SELECT MAX(calendar_date) FROM mobile_service_daily) AND customer_type IN ('Consumer', 'Business')), prepaid_canvas AS (SELECT DISTINCT customer_id, service_id, customer_type, CASE WHEN service_provider = 'Operator_A' AND NOT service_name LIKE '%Data%' THEN 'Plan_Tier_Voice_Service' WHEN service_provider = 'Operator_A' AND service_name LIKE '%Data%' THEN 'Plan_Tier_Data_Service' WHEN service_provider = 'Operator_B' AND NOT service_name LIKE '%Data%' THEN 'Plan_Tier_Alternative_Voice' ELSE 'Plan_Tier_Prepaid_Other' END AS plan_parent FROM prepaid_service_daily WHERE calendar_date = (SELECT MAX(calendar_date) FROM prepaid_service_daily) AND customer_type IN ('Consumer', 'Business')), broadband_active_status AS (SELECT ak.address_id, MAX(CASE WHEN ic1.service_connect_date >= CURRENT_DATE - INTERVAL '90' DAY THEN 'New_Customer' END) AS new_flag, MAX(CASE WHEN NOT ic2.location_id IS NULL THEN 'Active_Operator_A' END) AS active_flag, MAX(CASE WHEN ic2.location_id IS NULL THEN 'Inactive_Operator_A' END) AS inactive_flag FROM broadband_service_daily AS ic1 LEFT JOIN (SELECT id AS location_id, address_id FROM address_mapping GROUP BY id, address_id) AS ak ON ak.location_id = ic1.location_id LEFT JOIN (SELECT DISTINCT ic2.location_id, ic2.service_connect_date FROM broadband_service_daily AS ic2 WHERE calendar_date = (SELECT MAX(calendar_date) FROM broadband_service_daily) AND customer_type IN ('Consumer', 'Business')) AS ic2 ON ic1.location_id = ic2.location_id WHERE ic1.calendar_date >= CURRENT_DATE - INTERVAL '90' DAY AND ic1.customer_type IN ('Consumer', 'Business') GROUP BY ak.address_id), service_data_s1 AS (SELECT customer_id, service_id, customer_type, product_name, service_technology, location_id, plan_parent, CAST(NULL AS TEXT) AS plan_type_category, 'Broadband_Service' AS source FROM broadband_canvas UNION ALL SELECT customer_id, service_id, customer_type, NULL, NULL, NULL, plan_parent, NULL, 'Prepaid_Service' FROM prepaid_canvas UNION ALL SELECT customer_id, service_id, customer_type, NULL, NULL, NULL, plan_parent, plan_type_category, 'Mobile_Service' FROM mobile_canvas), service_tech AS (SELECT DISTINCT service_technology, location_id FROM service_data_s1), service_data AS (SELECT ak.address_id, CASE WHEN NOT ak.address_id IS NULL THEN LISTAGG(c.service_technology, ', ' ORDER BY c.service_technology) ELSE NULL END AS fixed_service_technology, MAX(CASE WHEN a.source = 'Broadband_Service' THEN 'Broadband_Service' END) AS broadband_service, MAX(CASE WHEN a.source = 'Mobile_Service' THEN 'Mobile_Service' END) AS mobile_service, MAX(CASE WHEN a.source = 'Prepaid_Service' THEN 'Prepaid_Service' END) AS prepaid_service, MAX(CASE WHEN a.plan_type_category = 'Data' THEN 'Mobile_Broadband' END) AS mobile_broadband, COUNT(DISTINCT CASE WHEN a.customer_type = 'Consumer' THEN a.customer_id ELSE NULL END) AS consumer_count, COUNT(DISTINCT CASE WHEN a.customer_type = 'Business' THEN a.customer_id ELSE NULL END) AS business_count, COUNT(DISTINCT CASE WHEN a.customer_type = 'Consumer' THEN a.service_id ELSE NULL END) AS consumer_services, COUNT(DISTINCT CASE WHEN a.customer_type = 'Business' THEN a.service_id ELSE NULL END) AS business_services, SUM(CASE WHEN a.plan_parent = 'Plan_Tier_Starter' THEN 1 ELSE 0 END) AS service_starter, SUM(CASE WHEN a.plan_parent = 'Plan_Tier_Basic' THEN 1 ELSE 0 END) AS service_basic, SUM(CASE WHEN a.plan_parent = 'Plan_Tier_Advanced' THEN 1 ELSE 0 END) AS service_advanced, SUM(CASE WHEN a.plan_parent = 'Plan_Tier_Premium' THEN 1 ELSE 0 END) AS service_premium, SUM(CASE WHEN a.plan_parent = 'Plan_Tier_Ultra' THEN 1 ELSE 0 END) AS service_ultra, SUM(CASE WHEN a.plan_parent = 'Plan_Tier_Elite' THEN 1 ELSE 0 END) AS service_elite, SUM(CASE WHEN a.plan_parent = 'Plan_Tier_High_Speed' THEN 1 ELSE 0 END) AS service_high_speed, SUM(CASE WHEN a.plan_parent = 'Plan_Tier_Satellite' THEN 1 ELSE 0 END) AS service_satellite, SUM(CASE WHEN a.plan_parent = 'Plan_Tier_Voice' THEN 1 ELSE 0 END) AS service_voice, SUM(CASE WHEN a.plan_parent = 'Plan_Tier_Other' THEN 1 ELSE 0 END) AS service_other, SUM(CASE WHEN a.plan_parent = 'Plan_Tier_Voice_Service' THEN 1 ELSE 0 END) AS prepaid_voice_service, SUM(CASE WHEN a.plan_parent = 'Plan_Tier_Data_Service' THEN 1 ELSE 0 END) AS prepaid_data_service, SUM(CASE WHEN a.plan_parent = 'Plan_Tier_Alternative_Voice' THEN 1 ELSE 0 END) AS prepaid_alternative, SUM(CASE WHEN a.plan_parent = 'Plan_Tier_Prepaid_Other' THEN 1 ELSE 0 END) AS prepaid_other, SUM(CASE WHEN a.plan_parent = 'Plan_Tier_Starter' THEN 1 ELSE 0 END) AS mobile_voice_starter, SUM(CASE WHEN a.plan_parent = 'Plan_Tier_Basic' THEN 1 ELSE 0 END) AS mobile_voice_basic, SUM(CASE WHEN a.plan_parent = 'Plan_Tier_Advanced' THEN 1 ELSE 0 END) AS mobile_voice_advanced, SUM(CASE WHEN a.plan_parent = 'Plan_Tier_Premium' THEN 1 ELSE 0 END) AS mobile_voice_premium, SUM(CASE WHEN a.plan_parent = 'Plan_Tier_Bundle' THEN 1 ELSE 0 END) AS mobile_voice_bundle, SUM(CASE WHEN a.plan_parent = 'Plan_Tier_Mobile_Other' THEN 1 ELSE 0 END) AS mobile_other, SUM(CASE WHEN a.plan_parent = 'Plan_Tier_Extra_Small' THEN 1 ELSE 0 END) AS mobile_data_xs, SUM(CASE WHEN a.plan_parent = 'Plan_Tier_Small' THEN 1 ELSE 0 END) AS mobile_data_s, SUM(CASE WHEN a.plan_parent = 'Plan_Tier_Medium' THEN 1 ELSE 0 END) AS mobile_data_m, SUM(CASE WHEN a.plan_parent = 'Plan_Tier_Large' THEN 1 ELSE 0 END) AS mobile_data_l, SUM(CASE WHEN a.plan_parent = 'Plan_Tier_Extra_Large' THEN 1 ELSE 0 END) AS mobile_data_xl, SUM(CASE WHEN a.plan_parent = 'Plan_Tier_Mobile_Data' THEN 1 ELSE 0 END) AS mobile_data_other FROM service_data_s1 AS a LEFT JOIN (SELECT customer_id, location_id, customer_type FROM account_daily WHERE calendar_date IN (SELECT MAX(calendar_date) FROM account_daily)) AS b ON a.customer_id = b.customer_id LEFT JOIN (SELECT DISTINCT id AS location_id, address_id FROM address_mapping) AS ak ON COALESCE(a.location_id, b.location_id) = ak.location_id LEFT JOIN service_tech AS c ON a.location_id = c.location_id GROUP BY ak.address_id), connectivity_household_canvas AS (SELECT DISTINCT hh.address_id, consumer_count, consumer_services, business_count, business_services, "State", city, postal_code, latitude, longitude, area_type, fixed_service_technology, CONCAT_WS(',', sd.broadband_service, sd.mobile_service, sd.prepaid_service, sd.mobile_broadband) AS service_portfolio, CONCAT_WS(',', CASE WHEN consumer_count > 0 THEN 'Consumer' END, CASE WHEN business_count > 0 THEN 'Business' END) AS customer_segment, al.location_category AS location_classification, service_starter, service_basic, service_advanced, service_premium, service_ultra, service_elite, service_high_speed, service_voice, service_satellite, service_other, prepaid_voice_service, prepaid_data_service, prepaid_alternative, prepaid_other, mobile_voice_starter, mobile_voice_basic, mobile_voice_advanced, mobile_voice_premium, mobile_voice_bundle, mobile_other, mobile_data_xs, mobile_data_s, mobile_data_m, mobile_data_l, mobile_data_xl, mobile_data_other, mobile_voice_starter + mobile_voice_basic + mobile_voice_advanced + mobile_voice_premium + mobile_voice_bundle + mobile_other + mobile_data_xs + mobile_data_s + mobile_data_m + mobile_data_l + mobile_data_xl + mobile_data_other + service_starter + service_basic + service_advanced + service_premium + service_ultra + service_elite + service_high_speed + service_satellite + service_other + service_voice + prepaid_voice_service + prepaid_data_service + prepaid_alternative + prepaid_other AS total_services_per_household, service_starter + service_basic + service_advanced + service_premium + service_ultra + service_elite + service_high_speed + service_satellite + service_other + service_voice AS fixed_services_per_household, mobile_voice_starter + mobile_voice_basic + mobile_voice_advanced + mobile_voice_premium + mobile_voice_bundle + mobile_data_xs + mobile_data_s + mobile_data_m + mobile_data_l + mobile_data_xl + mobile_data_other + mobile_other AS mobile_services_per_household, prepaid_voice_service + prepaid_data_service + prepaid_alternative + prepaid_other AS prepaid_services_per_household, CASE WHEN NOT new_flag IS NULL THEN new_flag WHEN NOT active_flag IS NULL THEN active_flag WHEN NOT inactive_flag IS NULL THEN inactive_flag WHEN service_starter + service_basic + service_advanced + service_premium + service_ultra + service_elite + service_high_speed + service_satellite + service_other > 0 THEN 'Active_Operator_A' ELSE 'Not_With_Operator_A' END AS broadband_status FROM household_profile_canvas AS hh LEFT JOIN broadband_active_status AS fs ON fs.address_id = hh.address_id LEFT JOIN service_data AS sd ON sd.address_id = hh.address_id LEFT JOIN (SELECT DISTINCT address_id, location_category FROM address_location) AS al ON al.address_id = hh.address_id WHERE calendar_date = (SELECT MAX(calendar_date) FROM household_profile_canvas)) SELECT hh.address_id, consumer_count, business_count, consumer_services, business_services, "state", city, postal_code, latitude, longitude, area_type, fixed_service_technology, nf.technology_type AS network_tech_type, CASE WHEN NOT service_status IS NULL THEN CASE WHEN service_status = 'Service_Status_A' THEN 'Network_Type_A' WHEN service_status = 'Service_Status_B' THEN 'Network_Type_B_Pending' WHEN service_status = 'Service_Status_C' THEN 'Network_Type_C_Not_Installed' WHEN service_status = 'Service_Status_D' THEN 'Network_Type_D_Not_Installed' WHEN technology_type = 'Network_Type_A' AND service_status = 'Service_Status_E' THEN 'Network_Type_E_Pending' WHEN technology_type = 'Network_Type_A' AND service_status = 'Service_Status_F' THEN 'Network_Type_F_Pending' WHEN technology_type = 'Network_Type_A' AND service_status = 'Service_Status_G' THEN 'Network_Type_G_Active' WHEN technology_type = 'Network_Type_B' AND service_status = 'Service_Status_H' THEN 'Network_Type_H_Pending' WHEN technology_type = 'Network_Type_B' AND service_status = 'Service_Status_I' THEN 'Network_Type_I_Pending' WHEN technology_type = 'Network_Type_B' AND service_status = 'Service_Status_G' THEN 'Network_Type_B_Active' WHEN service_status = 'Service_Status_J' THEN 'Network_Type_J_Active' WHEN service_status = 'Service_Status_K' THEN 'Network_Type_K_Pending' WHEN service_status = 'Service_Status_L' THEN 'Network_Type_L_Pending' WHEN service_status = 'Service_Status_M' THEN 'Network_Type_M_Pending' WHEN service_status = 'Service_Status_N' THEN 'Network_Type_N_Not_Installed' WHEN service_status = 'Service_Status_O' THEN 'Network_Type_O_Active' WHEN service_status = 'Service_Status_P' THEN 'Network_Type_P_Pending' WHEN service_status = 'Service_Status_Q' THEN 'Network_Type_Q_Pending' WHEN service_status = 'Service_Status_R' THEN 'Network_Type_R_Not_Serviceable' WHEN service_status = 'Network_Pending' THEN 'Network_Type_Pending' WHEN service_status = 'Service_Status_S' THEN 'Network_Type_S_Active' WHEN service_status = 'Service_Status_T' THEN 'Network_Type_T_Pending' WHEN service_status = 'Service_Status_U' THEN 'Network_Type_U_Pending' WHEN service_status = 'Service_Status_V' THEN 'Network_Type_V_Planned' WHEN service_status = 'Service_Status_W' THEN 'Network_Type_W_Planned' WHEN service_status = 'Service_Status_X' THEN 'Network_Type_X_Planned' ELSE service_status END ELSE 'Not_In_Network_Footprint' END AS service_class, service_restriction, service_status, service_portfolio, customer_segment, location_classification, CASE WHEN NOT nf.address_id IS NULL THEN 'Network_A' ELSE 'Network_Other' END AS network_type, segment_type, segment_group, affluence, household_composition, household_income, head_of_household_age, household_lifestage, child_young_probability, child_teen_probability, service_starter AS plan_starter, service_basic AS plan_basic, service_advanced AS plan_advanced, service_premium AS plan_premium, service_ultra, service_elite, service_high_speed, service_voice, service_satellite, service_other, prepaid_voice_service, prepaid_data_service, prepaid_alternative, prepaid_other, mobile_voice_starter, mobile_voice_basic, mobile_voice_advanced, mobile_voice_premium, mobile_voice_bundle, mobile_other, mobile_data_xs, mobile_data_s, mobile_data_m, mobile_data_l, mobile_data_xl, mobile_data_other, total_services_per_household, fixed_services_per_household, mobile_services_per_household, prepaid_services_per_household, broadband_status FROM connectivity_household_canvas AS hh LEFT JOIN (SELECT DISTINCT address_id, service_status, service_restriction, technology_type FROM broadband_footprint) AS nf ON nf.address_id = hh.address_id LEFT JOIN tbl_household_segmentation AS m ON m.address_id = hh.address_id
```

## EXPLAIN Plan

*EXPLAIN plan not available for this query. Use DAG cost percentages as proxy for bottleneck identification.*

## Query Structure (DAG)

### 1. broadband_canvas
**Role**: CTE (Definition Order: 0)
**Stats**: 0% Cost | ~0 rows
**Flags**: GROUP_BY
**Outputs**: [customer_id, service_id, product_code, product_name, service_technology, location_id, address_id, customer_type, plan_parent]
**Dependencies**: broadband_service_daily AS a (join), plan_mapping AS b (join), address_mapping (join), broadband_service_daily (correlated subquery)
**Filters**: calendar_date = (SELECT MAX(calendar_date) FROM broadband_service_daily) | customer_type IN ('Business', 'Consumer') | NOT ak.address_id IS NULL
**Key Logic (SQL)**:
```sql
SELECT
  a.customer_id,
  a.service_id,
  a.product_code,
  a.product_name,
  CASE WHEN NOT a.location_id IS NULL THEN a.technology_type ELSE NULL END AS service_technology,
  a.location_id,
  ak.address_id,
  a.customer_type,
  CASE
    WHEN b.plan_group = 'Plan_Group_Standard'
    THEN 'Plan_Tier_Standard'
    WHEN b.plan_group = 'Plan_Group_Basic'
    THEN 'Plan_Tier_Basic'
    WHEN b.plan_group = 'Plan_Group_Advanced'
    THEN 'Plan_Tier_Advanced'
    WHEN b.plan_group = 'Plan_Group_Premium'
    THEN 'Plan_Tier_Premium'
    WHEN b.plan_group = 'Plan_Group_High_Speed'
    THEN 'Plan_Tier_High_Speed'
...
```

### 2. mobile_canvas
**Role**: CTE (Definition Order: 0)
**Stats**: 0% Cost | ~0 rows
**Outputs**: [customer_id, service_id, customer_type, plan_type_category, plan_parent]
**Dependencies**: mobile_service_daily AS m (join), mobile_service_daily (correlated subquery)
**Filters**: calendar_date IN (SELECT MAX(calendar_date) FROM mobile_service_daily) | customer_type IN ('Consumer', 'Business')
**Key Logic (SQL)**:
```sql
SELECT
  customer_id,
  service_id,
  customer_type,
  CASE WHEN service_category = 'Data' THEN 'Mobile_Broadband' ELSE 'Mobile_Voice' END AS plan_type_category,
  CASE
    WHEN product_code IN (
      'Product_001',
      'Product_002',
      'Product_003',
      'Product_004',
      'Product_005',
      'Product_006'
    )
    THEN 'Plan_Tier_Starter'
    WHEN product_code IN (
      'Product_007',
      'Product_008',
      'Product_009',
      'Product_010',
...
```

### 3. prepaid_canvas
**Role**: CTE (Definition Order: 0)
**Stats**: 0% Cost | ~0 rows
**Outputs**: [customer_id, service_id, customer_type, plan_parent]
**Dependencies**: prepaid_service_daily
**Filters**: calendar_date = (SELECT MAX(calendar_date) FROM prepaid_service_daily) | customer_type IN ('Consumer', 'Business')
**Key Logic (SQL)**:
```sql
SELECT DISTINCT
  customer_id,
  service_id,
  customer_type,
  CASE
    WHEN service_provider = 'Operator_A' AND NOT service_name LIKE '%Data%'
    THEN 'Plan_Tier_Voice_Service'
    WHEN service_provider = 'Operator_A' AND service_name LIKE '%Data%'
    THEN 'Plan_Tier_Data_Service'
    WHEN service_provider = 'Operator_B' AND NOT service_name LIKE '%Data%'
    THEN 'Plan_Tier_Alternative_Voice'
    ELSE 'Plan_Tier_Prepaid_Other'
  END AS plan_parent
FROM prepaid_service_daily
WHERE
  calendar_date = (
    SELECT
      MAX(calendar_date)
    FROM prepaid_service_daily
  )
...
```

### 4. broadband_active_status
**Role**: CTE (Definition Order: 0)
**Stats**: 0% Cost | ~0 rows
**Flags**: GROUP_BY
**Outputs**: [address_id, new_flag, active_flag, inactive_flag]
**Dependencies**: broadband_service_daily AS ic1 (join), address_mapping (join), broadband_service_daily AS ic2 (join), broadband_service_daily (join)
**Filters**: ic1.calendar_date >= CURRENT_DATE - INTERVAL '90' DAY | ic1.customer_type IN ('Consumer', 'Business')
**Key Logic (SQL)**:
```sql
SELECT
  ak.address_id,
  MAX(
    CASE
      WHEN ic1.service_connect_date >= CURRENT_DATE - INTERVAL '90' DAY
      THEN 'New_Customer'
    END
  ) AS new_flag,
  MAX(CASE WHEN NOT ic2.location_id IS NULL THEN 'Active_Operator_A' END) AS active_flag,
  MAX(CASE WHEN ic2.location_id IS NULL THEN 'Inactive_Operator_A' END) AS inactive_flag
FROM broadband_service_daily AS ic1
LEFT JOIN (
  SELECT
    id AS location_id,
    address_id
  FROM address_mapping
  GROUP BY
    id,
    address_id
) AS ak
...
```

### 5. service_data_s1
**Role**: CTE (Definition Order: 1)
**Stats**: 0% Cost | ~0 rows
**Flags**: UNION_ALL
**Outputs**: [customer_id, service_id, customer_type, NULL, NULL, NULL, plan_parent, plan_type_category, 'Mobile_Service']
**Dependencies**: mobile_canvas
**Key Logic (SQL)**:
```sql
SELECT
  customer_id,
  service_id,
  customer_type,
  product_name,
  service_technology,
  location_id,
  plan_parent,
  CAST(NULL AS TEXT) AS plan_type_category,
  'Broadband_Service' AS source
FROM broadband_canvas
UNION ALL
SELECT
  customer_id,
  service_id,
  customer_type,
  NULL,
  NULL,
  NULL,
  plan_parent,
...
```

### 6. service_tech
**Role**: CTE (Definition Order: 2)
**Stats**: 0% Cost | ~0 rows
**Outputs**: [service_technology, location_id]
**Dependencies**: service_data_s1
**Key Logic (SQL)**:
```sql
SELECT DISTINCT
  service_technology,
  location_id
FROM service_data_s1
```

### 7. service_data
**Role**: CTE (Definition Order: 3)
**Stats**: 0% Cost | ~0 rows
**Flags**: GROUP_BY, ORDER_BY
**Outputs**: [address_id, fixed_service_technology, broadband_service, mobile_service, prepaid_service, mobile_broadband, consumer_count, business_count, consumer_services, business_services, ...] — ordered by c.service_technology ASC
**Dependencies**: service_data_s1 AS a (join), account_daily (join), address_mapping (join), service_tech AS c (join)
**Filters**: calendar_date IN (SELECT MAX(calendar_date) FROM account_daily)
**Key Logic (SQL)**:
```sql
SELECT
  ak.address_id,
  CASE
    WHEN NOT ak.address_id IS NULL
    THEN LISTAGG(c.service_technology, ', '
    ORDER BY
      c.service_technology)
    ELSE NULL
  END AS fixed_service_technology,
  MAX(CASE WHEN a.source = 'Broadband_Service' THEN 'Broadband_Service' END) AS broadband_service,
  MAX(CASE WHEN a.source = 'Mobile_Service' THEN 'Mobile_Service' END) AS mobile_service,
  MAX(CASE WHEN a.source = 'Prepaid_Service' THEN 'Prepaid_Service' END) AS prepaid_service,
  MAX(CASE WHEN a.plan_type_category = 'Data' THEN 'Mobile_Broadband' END) AS mobile_broadband,
  COUNT(DISTINCT CASE WHEN a.customer_type = 'Consumer' THEN a.customer_id ELSE NULL END) AS consumer_count,
  COUNT(DISTINCT CASE WHEN a.customer_type = 'Business' THEN a.customer_id ELSE NULL END) AS business_count,
  COUNT(DISTINCT CASE WHEN a.customer_type = 'Consumer' THEN a.service_id ELSE NULL END) AS consumer_services,
  COUNT(DISTINCT CASE WHEN a.customer_type = 'Business' THEN a.service_id ELSE NULL END) AS business_services,
  SUM(CASE WHEN a.plan_parent = 'Plan_Tier_Starter' THEN 1 ELSE 0 END) AS service_starter,
  SUM(CASE WHEN a.plan_parent = 'Plan_Tier_Basic' THEN 1 ELSE 0 END) AS service_basic,
  SUM(CASE WHEN a.plan_parent = 'Plan_Tier_Advanced' THEN 1 ELSE 0 END) AS service_advanced,
...
```

### 8. connectivity_household_canvas
**Role**: CTE (Definition Order: 4)
**Stats**: 0% Cost | ~0 rows
**Outputs**: [address_id, consumer_count, consumer_services, business_count, business_services, State, city, postal_code, latitude, longitude, ...]
**Dependencies**: household_profile_canvas AS hh (join), broadband_active_status AS fs (join), service_data AS sd (join), address_location (join), household_profile_canvas (correlated subquery)
**Filters**: calendar_date = (SELECT MAX(calendar_date) FROM household_profile_canvas)
**Key Logic (SQL)**:
```sql
SELECT DISTINCT
  hh.address_id,
  consumer_count,
  consumer_services,
  business_count,
  business_services,
  "State",
  city,
  postal_code,
  latitude,
  longitude,
  area_type,
  fixed_service_technology,
  CONCAT_WS(',', sd.broadband_service, sd.mobile_service, sd.prepaid_service, sd.mobile_broadband) AS service_portfolio,
  CONCAT_WS(
    ',',
    CASE WHEN consumer_count > 0 THEN 'Consumer' END,
    CASE WHEN business_count > 0 THEN 'Business' END
  ) AS customer_segment,
  al.location_category AS location_classification,
...
```

### 9. main_query
**Role**: Root / Output (Definition Order: 5)
**Stats**: 0% Cost | ~0 rows
**Flags**: GROUP_BY
**Outputs**: [address_id, consumer_count, business_count, consumer_services, business_services, state, city, postal_code, latitude, longitude, ...]
**Dependencies**: connectivity_household_canvas AS hh (join), broadband_footprint (join), tbl_household_segmentation AS m (join)
**Key Logic (SQL)**:
```sql
SELECT
  hh.address_id,
  consumer_count,
  business_count,
  consumer_services,
  business_services,
  "state",
  city,
  postal_code,
  latitude,
  longitude,
  area_type,
  fixed_service_technology,
  nf.technology_type AS network_tech_type,
  CASE
    WHEN NOT service_status IS NULL
    THEN CASE
      WHEN service_status = 'Service_Status_A'
      THEN 'Network_Type_A'
      WHEN service_status = 'Service_Status_B'
...
```

### Edges
- mobile_canvas → service_data_s1
- broadband_canvas → service_data_s1
- prepaid_canvas → service_data_s1
- service_data_s1 → service_tech
- service_data_s1 → service_data
- service_tech → service_data
- broadband_active_status → connectivity_household_canvas
- service_data → connectivity_household_canvas
- connectivity_household_canvas → main_query


## Aggregation Semantics Check

You MUST verify aggregation equivalence for any proposed restructuring:

- **STDDEV_SAMP(x)** requires >=2 non-NULL values per group. Returns NULL for 0-1 values. Changing group membership changes the result.
- `STDDEV_SAMP(x) FILTER (WHERE year=1999)` over a combined (1999,2000) group is NOT equivalent to `STDDEV_SAMP(x)` over only 1999 rows — FILTER still uses the combined group's membership for the stddev denominator.
- **AVG and STDDEV are NOT duplicate-safe**: if a join introduces row duplication, the aggregate result changes.
- When splitting a UNION ALL CTE with GROUP BY + aggregate, each split branch must preserve the exact GROUP BY columns and filter to the exact same row set as the original.
- **SAFE ALTERNATIVE**: If GROUP BY includes the discriminator column (e.g., d_year), each group is already partitioned. STDDEV_SAMP computed per-group is correct. You can then pivot using `MAX(CASE WHEN year = 1999 THEN year_total END) AS year_total_1999` because the GROUP BY guarantees exactly one row per (customer, year) — the MAX is just a row selector, not a real aggregation.

## Your Task

First, use a `<reasoning>` block for your internal analysis. This will be stripped before parsing. Work through these steps IN ORDER:

1. **CLASSIFY**: What structural archetype is this query?
   (channel-comparison self-join / correlated-aggregate filter / star-join with late dim filter / repeated fact scan / multi-channel UNION ALL / EXISTS-set operations / other)

2. **EXPLAIN PLAN ANALYSIS**: From the EXPLAIN ANALYZE output, identify:
   - Compute wall-clock ms per EXPLAIN node. Sum repeated operations (e.g., 2x store_sales joins = total cost). The EXPLAIN is ground truth, not the DAG cost percentages.
   - Which nodes consume >10% of runtime and WHY
   - Where row counts drop sharply (existing selectivity)
   - Where row counts DON'T drop (missed optimization opportunity)
   - Whether the optimizer already splits CTEs, pushes predicates, or performs transforms you might otherwise assign
   - Count scans per base table. If a fact table is scanned N times, a restructuring that reduces it to 1 scan saves (N-1)/N of that table's I/O cost. Prioritize transforms that reduce scan count on the largest tables.
   - Whether the CTE is materialized once and probed multiple times, or re-executed per reference

3. **GAP MATCHING**: Compare the EXPLAIN analysis to the Engine Profile gaps above. For each gap:
   - Does this query exhibit the gap? (e.g., is a predicate NOT pushed into a CTE? Is the same fact table scanned multiple times?)
   - Check the 'opportunity' — does this query's structure match?
   - Check 'what_didnt_work' and 'field_notes' — any disqualifiers for this query?
   - Also verify: is the optimizer ALREADY handling this well? (Check the Optimizer Strengths above — if the engine already does it, your transform adds overhead, not value.)

4. **AGGREGATION TRAP CHECK**: For every aggregate function in the query, verify: does my proposed restructuring change which rows participate in each group? STDDEV_SAMP, VARIANCE, PERCENTILE_CONT, CORR are grouping-sensitive. SUM, COUNT, MIN, MAX are grouping-insensitive (modulo duplicates). If the query uses FILTER clauses or conditional aggregation, verify equivalence explicitly.

5. **TRANSFORM SELECTION**: From the matched engine gaps, select the single best transform (or compound strategy) that maximizes expected value (rows affected × historical speedup from evidence) for THIS query.
   REJECT tag-matched examples whose primary technique requires a structural feature this query lacks. Tag matching is approximate — always verify structural applicability.

6. **DAG DESIGN**: Define the target DAG topology for your chosen strategy. Verify that every node contract has exhaustive output columns by checking downstream references.
   CTE materialization matters: a CTE referenced by 2+ consumers will likely be materialized. A CTE referenced once may be inlined.

7. **WRITE REWRITE**: Implement your strategy as a JSON rewrite_set. Each changed or added CTE is a node. Produce per-node SQL matching your DAG design from step 6. Declare output columns for every node in `node_contracts`. The rewrite must be semantically equivalent to the original.

Then produce the structured briefing in EXACTLY this format:

```
=== SHARED BRIEFING ===

SEMANTIC_CONTRACT: (80-150 tokens, cover ONLY:)
(a) One sentence of business intent (start from pre-computed intent if available).
(b) JOIN type semantics that constrain rewrites (INNER = intersection = all sides must match).
(c) Any aggregation function traps specific to THIS query.
(d) Any filter dependencies that a rewrite could break.
Do NOT repeat information already in ACTIVE_CONSTRAINTS or REGRESSION_WARNINGS.

BOTTLENECK_DIAGNOSIS:
[Which operation dominates cost and WHY (not just '50% cost').
Scan-bound vs join-bound vs aggregation-bound.
Cardinality flow (how many rows at each stage).
What the optimizer already handles well (don't re-optimize).
Whether DAG cost percentages are misleading.]

ACTIVE_CONSTRAINTS:
- [CORRECTNESS_CONSTRAINT_ID]: [Why it applies to this query, 1 line]
- [ENGINE_GAP_ID]: [Evidence from EXPLAIN that this gap is active]
(List all 4 correctness constraints + the 1-3 engine gaps that
are active for THIS query based on your EXPLAIN analysis.)

REGRESSION_WARNINGS:
1. [Pattern name] ([observed regression]):
   CAUSE: [What happened mechanistically]
   RULE: [Actionable avoidance rule for THIS query]
(If no regression warnings are relevant, write 'None applicable.')

=== REWRITE ===

```json
{
  "rewrite_sets": [{
    "id": "rs_01",
    "transform": "<transform_name>",
    "nodes": {
      "<cte_name>": "<SQL for this CTE body>",
      "main_query": "<final SELECT>"
    },
    "node_contracts": {
      "<cte_name>": ["col1", "col2", "..."],
      "main_query": ["col1", "col2", "..."]
    },
    "set_local": ["SET LOCAL work_mem = '512MB'", "SET LOCAL jit = 'off'"],
    "data_flow": "<cte_a> -> <cte_b> -> main_query",
    "invariants_kept": ["same output columns", "same rows"],
    "expected_speedup": "2.0x",
    "risk": "low"
  }]
}
```

Rules:
- Every node in `nodes` MUST appear in `node_contracts` and vice versa
- `node_contracts`: list the output column names each node produces
- `data_flow`: show the CTE dependency chain
- `main_query` = the final SELECT
- Only include nodes you changed or added; unchanged nodes auto-filled from original

After the JSON, explain the mechanism:

```
Changes: <1-2 sentences: what structural change + the expected mechanism>
Expected speedup: <estimate>
```
```

## Section Validation Checklist (MUST pass before final output)

Use this checklist to verify content quality, not just section presence:

### SHARED BRIEFING
- `SEMANTIC_CONTRACT`: 80-150 tokens and includes business intent, JOIN semantics, aggregation trap, and filter dependency.
- `BOTTLENECK_DIAGNOSIS`: states dominant mechanism, bound type (`scan-bound`/`join-bound`/`aggregation-bound`), cardinality flow, and what optimizer already handles well.
- `ACTIVE_CONSTRAINTS`: includes all 4 correctness IDs plus 1-3 active engine gaps with EXPLAIN evidence.
- `REGRESSION_WARNINGS`: either `None applicable.` or numbered entries with both `CAUSE:` and `RULE:`.

### REWRITE
- JSON `rewrite_sets` block is present with at least one rewrite set.
- `transform`: non-empty, names the optimization transform.
- `nodes`: every changed/added CTE has per-node SQL.
- `node_contracts`: every node in `nodes` has a matching contract with output column list.
- `data_flow`: shows the CTE dependency chain.
- `main_query` output columns match original query exactly (same names, same order).
- All literals preserved exactly (numbers, strings, date values).
- Semantically equivalent to the original query.

## Transform Catalog

Select the best transform (or compound strategy of 2-3 transforms) that maximizes expected speedup for THIS query.

### Predicate Movement
- **global_predicate_pushdown**: Trace selective predicates from late in the CTE chain back to the earliest scan via join equivalences. Biggest win when a dimension filter is applied after a large intermediate materialization.
  Maps to examples: pushdown, early_filter, date_cte_isolate
- **transitive_predicate_propagation**: Infer predicates through join equivalence chains (A.key = B.key AND B.key = 5 -> A.key = 5). Especially across CTE boundaries where optimizers stop propagating.
  Maps to examples: early_filter, dimension_cte_isolate
- **null_rejecting_join_simplification**: When downstream WHERE rejects NULLs from the outer side of a LEFT JOIN, convert to INNER. Enables reordering and predicate pushdown. CHECK: does the query actually have LEFT/OUTER joins before assigning this.
  Maps to examples: (no direct gold example — novel transform)

### Join Restructuring
- **self_join_elimination**: When a UNION ALL CTE is self-joined N times with each join filtering to a different discriminator, split into N pre-partitioned CTEs. Eliminates discriminator filtering and repeated hash probes on rows that don't match.
  Maps to examples: union_cte_split, shared_dimension_multi_channel
- **decorrelation**: Convert correlated EXISTS/IN/scalar subqueries to CTE + JOIN. CHECK: does the query actually have correlated subqueries before assigning this.
  Maps to examples: decorrelate, composite_decorrelate_union
- **aggregate_pushdown**: When GROUP BY follows a multi-table join but aggregation only uses columns from one side, push the GROUP BY below the join. CHECK: verify the join doesn't change row multiplicity for the aggregate (one-to-many breaks AVG/STDDEV).
  Maps to examples: (no direct gold example — novel transform)
- **late_attribute_binding**: When a dimension table is joined only to resolve display columns (names, descriptions) that aren't used in filters, aggregations, or join conditions, defer that join until after all filtering and aggregation is complete. Join on the surrogate key once against the final reduced result set. This eliminates N-1 dimension scans when the CTE references the dimension N times. CHECK: verify the deferred columns aren't used in WHERE, GROUP BY, or JOIN ON — only in the final SELECT.
  Maps to examples: dimension_cte_isolate (partial pattern), early_filter

### Scan Optimization
- **star_join_prefetch**: Pre-filter ALL dimension tables into CTEs, then probe fact table with the combined key intersection.
  Maps to examples: dimension_cte_isolate, multi_dimension_prefetch, prefetch_fact_join, date_cte_isolate
- **single_pass_aggregation**: Merge N subqueries on the same fact table into 1 scan with CASE/FILTER inside aggregates. CHECK: STDDEV_SAMP/VARIANCE are grouping-sensitive — FILTER over a combined group != separate per-group computation.
  Maps to examples: single_pass_aggregation, channel_bitmap_aggregation
- **scan_consolidation_pivot**: When a CTE is self-joined N times with each reference filtering to a different discriminator (e.g., year, channel), consolidate into fewer scans that GROUP BY the discriminator, then pivot rows to columns using MAX(CASE WHEN discriminator = X THEN agg_value END). This halves the fact scans and dimension joins. SAFE when GROUP BY includes the discriminator — each group is naturally partitioned, so aggregates like STDDEV_SAMP are computed correctly per-partition. The pivot MAX is just a row selector (one row per group), not a real aggregation.
  Maps to examples: single_pass_aggregation, union_cte_split

### Structural Transforms
- **union_consolidation**: Share dimension lookups across UNION ALL branches that scan different fact tables with the same dim joins.
  Maps to examples: shared_dimension_multi_channel
- **window_optimization**: Push filters before window functions when they don't affect the frame. Convert ROW_NUMBER + filter to LATERAL + LIMIT. Merge same-PARTITION windows into one sort pass.
  Maps to examples: deferred_window_aggregation
- **exists_restructuring**: Convert INTERSECT to EXISTS for semi-join short-circuit, or restructure complex EXISTS with shared CTEs. CHECK: does the query actually have INTERSECT or complex EXISTS.
  Maps to examples: intersect_to_exists, multi_intersect_exists_cte

## Strategy Selection Rules

1. **CHECK APPLICABILITY**: Each transform has a structural prerequisite (correlated subquery, UNION ALL CTE, LEFT JOIN, etc.). Verify the query actually has the prerequisite before assigning a transform. DO NOT assign decorrelation if there are no correlated subqueries.
2. **CHECK OPTIMIZER OVERLAP**: Read the EXPLAIN plan. If the optimizer already performs a transform (e.g., already splits a UNION CTE, already pushes a predicate), that transform will have marginal benefit. Note this in your reasoning and prefer transforms the optimizer is NOT already doing.
3. **MAXIMIZE EXPECTED VALUE**: Select the single strategy with the highest expected speedup, considering both the magnitude of the bottleneck it addresses and the historical success rate.
4. **ASSESS RISK PER-QUERY**: Risk is a function of (transform x query complexity), not an inherent property of the transform. Decorrelation is low-risk on a simple EXISTS and high-risk on nested correlation inside a CTE. Assess per-assignment.
5. **COMPOSITION IS ALLOWED AND ENCOURAGED**: A strategy can combine 2-3 transforms from different categories (e.g., star_join_prefetch + scan_consolidation_pivot, or date_cte_isolate + early_filter + decorrelate). The TARGET_DAG should reflect the combined structure. Compound strategies are often the source of the biggest wins.

Select 1-3 examples that genuinely match the strategy. Do NOT pad with irrelevant examples — an irrelevant example is worse than no example. Use example IDs from the catalog above.

For TARGET_DAG: Define the CTE structure you want produced. For NODE_CONTRACTS: Be exhaustive with OUTPUT columns — missing columns cause semantic breaks.
