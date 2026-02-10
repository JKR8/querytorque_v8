SELECT DISTINCT i_product_name
FROM item i1
WHERE i_manufact_id BETWEEN 748 AND 788
  AND EXISTS (
    SELECT 1
    FROM item i2
    WHERE i2.i_manufact = i1.i_manufact
      AND (
        (i2.i_category = 'Women' AND i2.i_color IN ('gainsboro', 'aquamarine') AND i2.i_units IN ('Ounce', 'Dozen') AND i2.i_size IN ('medium', 'economy'))
        OR (i2.i_category = 'Women' AND i2.i_color IN ('chiffon', 'violet') AND i2.i_units IN ('Ton', 'Pound') AND i2.i_size IN ('extra large', 'small'))
        OR (i2.i_category = 'Men' AND i2.i_color IN ('chartreuse', 'blue') AND i2.i_units IN ('Each', 'Oz') AND i2.i_size IN ('N/A', 'large'))
        OR (i2.i_category = 'Men' AND i2.i_color IN ('tan', 'dodger') AND i2.i_units IN ('Bunch', 'Tsp') AND i2.i_size IN ('medium', 'economy'))
        OR (i2.i_category = 'Women' AND i2.i_color IN ('blanched', 'tomato') AND i2.i_units IN ('Tbl', 'Case') AND i2.i_size IN ('medium', 'economy'))
        OR (i2.i_category = 'Women' AND i2.i_color IN ('almond', 'lime') AND i2.i_units IN ('Box', 'Dram') AND i2.i_size IN ('extra large', 'small'))
        OR (i2.i_category = 'Men' AND i2.i_color IN ('peru', 'saddle') AND i2.i_units IN ('Pallet', 'Gram') AND i2.i_size IN ('N/A', 'large'))
        OR (i2.i_category = 'Men' AND i2.i_color IN ('indian', 'spring') AND i2.i_units IN ('Unknown', 'Carton') AND i2.i_size IN ('medium', 'economy'))
      )
  )
ORDER BY i_product_name
LIMIT 100;