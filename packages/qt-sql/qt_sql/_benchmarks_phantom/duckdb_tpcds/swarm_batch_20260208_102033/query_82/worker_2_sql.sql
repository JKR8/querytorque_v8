WITH filtered_items AS (
    SELECT i_item_sk, i_item_id, i_item_desc, i_current_price
    FROM item
    WHERE i_current_price BETWEEN 17 AND 17 + 30
      AND i_manufact_id IN (639, 169, 138, 339)
), filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_date BETWEEN CAST('1999-07-09' AS DATE) AND (
        CAST('1999-07-09' AS DATE) + INTERVAL '60' DAY
    )
)
SELECT
    fi.i_item_id,
    fi.i_item_desc,
    fi.i_current_price
FROM filtered_items fi
JOIN inventory inv ON fi.i_item_sk = inv.inv_item_sk
JOIN filtered_dates fd ON inv.inv_date_sk = fd.d_date_sk
JOIN store_sales ss ON fi.i_item_sk = ss.ss_item_sk
WHERE inv.inv_quantity_on_hand BETWEEN 100 AND 500
GROUP BY
    fi.i_item_id,
    fi.i_item_desc,
    fi.i_current_price
ORDER BY
    fi.i_item_id
LIMIT 100