REGISTER /opt/cloudera/parcels/CDH/lib/pig/piggybank.jar;
DEFINE RegexMatch org.apache.pig.piggybank.evaluation.string.RegexMatch();
mkdir hw5;
copyFromLocal input.txt hw5/input.txt; --input.txt contains data to be searched
copyFromLocal terms.txt hw5/terms.txt; --terms.txt contains the strings to search for
lines = LOAD 'hw5/input.txt' AS (line:chararray);
words = LOAD 'hw5/terms.txt' AS (word:chararray);
data = CROSS words, lines;
results = FOREACH data GENERATE words::word, RegexMatch(LOWER(lines::line), CONCAT('.*', LOWER(words::word), '.*'));
grouped_results = FOREACH (GROUP results BY words::word) GENERATE group, SUM(results.$1);
rmf hw5/output;
STORE (ORDER grouped_results BY $0) INTO 'hw5/output';
sh rm -rf output;
copyToLocal hw5/output output;
sh cat output/* > output.txt;
