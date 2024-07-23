-- Load the CSV file
salesTable = LOAD 'hdfs:///user/AnshulGupta/sales.csv' USING PigStorage(',') AS (Product:chararray,Price:chararray,Payment_Type:chararray,Name:chararray,City:chararray,State:chararray,Country:chararray);
-- Group data using the country column
GroupByCountry = GROUP salesTable BY Country;
-- Generate result format
CountByCountry = FOREACH GroupByCountry GENERATE CONCAT((chararray)$0, CONCAT(':', (chararray)COUNT($1)));
-- Save result in HDFS folder
STORE CountByCountry INTO 'salesOutput' USING PigStorage('\t');


//input
Product,Price,Payment_Type,Name,City,State,Country
Product1,1200,Mastercard,carolina,Basildon,England,United Kingdom
Product1,1200,Visa,Betina,Parkville,MO,United States
Product1,1200,Mastercard,Federica e Andrea,Astoria,OR,United States
Product1,1200,Visa,Gouya,Echuca,Victoria,Australia
Product2,3600,Visa,Gerd W ,Cahaba Heights,AL,United States
Product1,1200,Visa,LAURENCE,Mickleton,NJ,United States
Product1,1200,Mastercard,Fleur,Peoria ,IL,United States
Product1,1200,Mastercard,adam,Martin ,TN,United States
Product1,1200,Mastercard,Renee Elisabeth,Tel Aviv,Tel Aviv,Israel
Product1,1200,Visa,Aidan,Chatou,Ile-de-France,France
Product1,1200,Diners,Stacy,New York ,NY,United States
Product1,1200,Amex,Heidi,Eindhoven,Noord-Brabant,Netherlands
Product1,1200,Mastercard,Sean ,Shavano Park,TX,United States
Product1,1200,Visa,Georgia,Eagle,ID,United States
