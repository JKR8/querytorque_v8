WITH qualifying_manufacturers AS (
    SELECT DISTINCT i_manufact
    FROM item
    WHERE (
        (i_category = 'Women' AND i_color IN ('gainsboro', 'aquamarine') AND i_units IN ('Ounce', 'Dozen') AND i_size IN ('medium', 'economy'))
        OR (i_category = 'Women' AND i_color IN ('chiffon', 'violet') AND i_units IN ('Ton', 'Pound') AND i_size IN ('extra large', 'small'))
        OR (i_category = 'Men' AND i_color IN ('chartreuse', 'blue') AND i_units IN ('Each', 'Oz') AND i_size IN ('N/A', 'large'))
        OR (i_category = 'Men' AND i_color IN ('tan', 'dodger') AND i_units IN ('Bunch', 'Tsp') AND i_size IN ('medium', 'economy'))
        OR (i_category = 'Women' AND i_color IN ('blanched', 'tomato') AND i_units IN ('Tbl', 'Case') AND i_size IN ('medium', 'economy'))
        OR (i_category = 'Women' AND i_color IN ('almond', 'lime') AND i_units IN ('Box', 'Dram') AND i_size IN ('extra large', 'small'))
        OR (i_category = 'Men' AND i_color IN ('peru', 'saddle') AND i_units IN ('Pallet', 'Gram') AND i_size IN ('N/A', 'large'))
        OR (i_category = 'Men' AND i_color IN ('indian', 'spring') AND i_units IN ('Unknown', 'Carton') AND i_size IN ('medium', 'economy'))
    )
)
SELECT DISTINCT
  i_product_name
FROM item AS i1
WHERE
  i_manufact_id BETWEEN 748 AND 788
  AND i_manufact IN (SELECT i_manufact FROM qualifying_manufacturers)
ORDER BY
  i_product_name
LIMIT 100