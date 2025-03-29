-----------------------------------------------
--1. Get the Latest Record per Group

SELECT *
FROM main_table m
JOIN (
  SELECT group_id, MAX(timestamp_column) AS latest_time
  FROM main_table
  GROUP BY group_id
) subq
ON m.group_id = subq.group_id AND m.timestamp_column = subq.latest_time;


-----------------------------------------------
--2. Find Duplicates

SELECT key_column, COUNT(*)
FROM some_table
GROUP BY key_column
HAVING COUNT(*) > 1;


-----------------------------------------------
--3. Generate Date Spine (Fill Missing Dates)

SELECT d.date_value, t.*
FROM date_spine d
LEFT JOIN some_table t ON t.date_column = d.date_value;


-----------------------------------------------
--4. Aggregate with Conditions (CASE in Aggregation)

SELECT 
  group_id,
  COUNT(*) AS total_count,
  COUNT(CASE WHEN condition_column = 'value' THEN 1 END) AS value_count
FROM some_table
GROUP BY group_id;


-----------------------------------------------
--5. Find NULLs in Any Column

SELECT *
FROM some_table
WHERE col1 IS NULL OR col2 IS NULL OR col3 IS NULL;


-----------------------------------------------
--6. Find Gaps in Sequential IDs

SELECT current_id + 1 AS missing_id
FROM (
  SELECT id_column, LEAD(id_column) OVER (ORDER BY id_column) AS next_id
  FROM some_table
) subq
WHERE next_id - id_column > 1;


-----------------------------------------------
--7. Windowed Running Total

SELECT *,
  SUM(value_column) OVER (PARTITION BY group_id ORDER BY timestamp_column) AS running_total
FROM some_table;


-----------------------------------------------
--8. Rank Within Groups

SELECT *,
  RANK() OVER (PARTITION BY group_id ORDER BY sort_column DESC) AS group_rank
FROM some_table;


-----------------------------------------------
--9. Filter First Value in Window

SELECT *,
  FIRST_VALUE(value_column) OVER (PARTITION BY group_id ORDER BY timestamp_column) AS first_val
FROM some_table;


-----------------------------------------------
--10. Unpivot Columns (Wide to Long)

SELECT id_column, 'col1' AS column_name, col1 AS column_value FROM some_table
UNION ALL
SELECT id_column, 'col2', col2 FROM some_table
UNION ALL
SELECT id_column, 'col3', col3 FROM some_table;


-----------------------------------------------
--11. Pivot (Long to Wide)

SELECT group_id,
  MAX(CASE WHEN label_column = 'label1' THEN value_column END) AS label1_value,
  MAX(CASE WHEN label_column = 'label2' THEN value_column END) AS label2_value
FROM long_table
GROUP BY group_id;


-----------------------------------------------
--12. Join with Latest Matching Record

SELECT a.*, b.*
FROM table_a a
LEFT JOIN (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY foreign_id ORDER BY timestamp_column DESC) AS rn
  FROM table_b
) b ON a.id = b.foreign_id AND b.rn = 1;


-----------------------------------------------
--13. Conditional Join on Ranges

SELECT *
FROM range_table r
JOIN point_table p
  ON p.value_column BETWEEN r.range_start AND r.range_end;


-----------------------------------------------
--14. Self-Join to Compare Rows

SELECT a.id AS id_a, b.id AS id_b, a.value_column, b.value_column
FROM same_table a
JOIN same_table b
  ON a.group_id = b.group_id AND a.id < b.id;


-----------------------------------------------
--15. Find Percentiles (Using NTILE or PERCENT_RANK)

SELECT *,
  NTILE(4) OVER (ORDER BY value_column) AS quartile
FROM some_table;


-----------------------------------------------
--16. Identify Consecutive Events

SELECT *,
  SUM(change_flag) OVER (ORDER BY timestamp_column) AS group_seq
FROM (
  SELECT *,
    CASE WHEN condition_column = 'target' THEN 0 ELSE 1 END AS change_flag
  FROM some_table
) subq;


-----------------------------------------------
--17. Deduplicate Rows Keeping the Most Recent
Problem: Keep latest version per key.

SELECT *
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY key_column ORDER BY timestamp_column DESC) AS rn
  FROM some_table
) subq
WHERE rn = 1;


-----------------------------------------------
--18. Join Multiple Tables Efficiently
Problem: Join three or more related tables.

SELECT a.*, b.*, c.*
FROM table_a a
JOIN table_b b ON a.key = b.key
JOIN table_c c ON b.key2 = c.key2;


-----------------------------------------------
--19. Check if a Value Exists in Any Column

SELECT *
FROM some_table
WHERE 'target_value' IN (col1, col2, col3);


-----------------------------------------------
--20. Optimize Large Aggregations with Temp Tables

CREATE TEMP TABLE temp_agg AS
SELECT group_id, SUM(value_column) AS total
FROM some_table
GROUP BY group_id;

SELECT *
FROM temp_agg
WHERE total > 1000;