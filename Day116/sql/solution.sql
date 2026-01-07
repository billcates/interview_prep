select lower(regexp_split_to_table(
    regexp_replace(t.contents, '[[:punct:]]', '', 'g'),
    E'\\s+'
)) AS word ,count(1) as occurrences
from google_file_store t
group by 1
order by 2 desc;