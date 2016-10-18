records = LOAD 'task_1d/part-r-00000' USING PigStorage('\t')
    AS (word:chararray, count:int);
records_sorted = ORDER records BY count DESC;
top_10 = LIMIT records_sorted 10;
DUMP top_10;