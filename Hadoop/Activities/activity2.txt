-- Load input file from HDFS
inputFile = LOAD 'hdfs:///user/nagendra/file01.txt' AS (line);
-- Tokeize each word in the file (Map)
words = FOREACH inputFile GENERATE FLATTEN(TOKENIZE(line)) AS word;
-- Combine the words from the above stage
grpd = GROUP words BY word;
-- Count the occurence of each word (Reduce)
cntd = FOREACH grpd GENERATE group,$0, COUNT(words);
-- Remove the old results
rmf 'hdfs:///user/nagendra/results';
-- Store the result in HDFS
STORE cntd INTO 'hdfs:///user/nagendra/results';