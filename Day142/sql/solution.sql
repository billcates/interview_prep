WITH duplicates AS (
  SELECT id
  FROM (
    SELECT id,
           ROW_NUMBER() OVER (PARTITION BY email ORDER BY id) AS rn
    FROM person
  ) t
  WHERE rn > 1
)
DELETE FROM person
WHERE id IN (SELECT id FROM duplicates);


-- delete from table a join table b is not supported in postgres
