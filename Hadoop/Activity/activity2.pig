-- Load input file from HDFS
inputFile = LOAD 'hdfs:///user/anshulgupta/input.txt' AS (line:chararray);
-- Tokeize the lines into words(Map)
words = FOREACH inputFile GENERATE FLATTEN(TOKENIZE(line)) AS word;
-- Combine the words from the above stage
grpd = GROUP words BY word;
-- Count the occurence of each word (Reduce)
totalCount = FOREACH grpd GENERATE $0, COUNT($1);
-- Store the result in HDFS
STORE totalCount INTO 'hdfs:///user/anshulgupta/PigOutput1';


This is an example file
This is an example line

//Expected output
$0      ,$1
This    ,2
is      ,2
an      ,2
example ,2
file    ,1
line    ,1 
