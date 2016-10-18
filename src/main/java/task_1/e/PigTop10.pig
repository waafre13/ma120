/*
1.e) Pig top 10. Write a Pig script that selects the top 10 list words after
you remove the stopwords in step d).
*/

/* Get the list, identify data separators as tab (\t) */
list = LOAD 'task_1d/part-r-00000' USING PigStorage('\t')
    AS (word:chararray, count:int);

/* Sort the list by word occurences in a descending order */
sorted_list = ORDER list BY count DESC;

/* Limit the list by the 10 first lines */
top_10 = LIMIT sorted_list 10;

/* Write the "top_10-list" to console */
DUMP top_10;