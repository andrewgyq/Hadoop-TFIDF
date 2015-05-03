Reference: https://code.google.com/p/hadoop-clusternet/wiki/RunningMapReduceExampleTFIDF

------------------------------------------------------------------------------------------------
How to run: run.sh
#!/bin/bash
echo "create folder"
mkdir tfidf_classes
echo "put books into HDFS"
hadoop dfs -mkdir books
hadoop dfs -put books/* books
echo "compile"
javac -classpath hadoop-1.2.1/hadoop-core-1.2.1.jar:hadoop-1.2.1/lib/commons-cli-1.2.jar -d tfidf_classes TFIDF.java 
echo "generate jar"
jar cvf tfidf.jar -C tfidf_classes/ .
echo "remove output folder"
hadoop dfs -rmr temp1
hadoop dfs -rmr temp2
hadoop dfs -rmr temp3
hadoop dfs -rmr output
echo "run hadoop job"
hadoop jar tfidf.jar TFIDF query.txt books output
echo "show result"
hadoop dfs -cat temp3/part-r-00000 > tfidf.txt
hadoop dfs -cat output/part-r-00000 > output.txt
cat output.txt 

--------------------------------------------------------------------------------------------------------
MapReduce jobs:
1. TermFrequency: Count how many times a given term occurs in a document.
input: books (24 files)
output: temp1 
acceded@pg1342.txt	1
acceded@pg158.txt	1
acceded@pg4300.txt	3
acceded@pg84.txt	1

2. WordCountInDoc: Count the total number of terms in document
input: temp1
output temp2
acceded@pg1342.txt	1/124588
acceded@pg158.txt	1/160449
acceded@pg4300.txt	3/267976
acceded@pg84.txt	1/77986

3. DocumentFrequencyTFIDF: Compute document frequency and the calculation of the TF-IDF
input: temp2
output temp3
acceded@pg1342.txt	0.00000625
acceded@pg158.txt	0.00000485
acceded@pg4300.txt	0.00000871
acceded@pg84.txt	0.00000998

4. Query: Query based on query,txt and output is the filename with the highest tfidf score of the term
input: temp3
output: output
acceded	0.00000998 pg84.txt

---------------------------------------------------------------------------------------------------
If a term appears in every document, such as 'actual', its inverse document frequency is 0 and its tfidf value is 0,
the result is any document of the document sets. This term may be stopword.
actual@pg84.txt	0
actual@pg2591.txt	0
actual@pg105.txt	0
actual@pg1232.txt	0
actual@pg1342.txt	0
actual@pg23.txt	0
actual@pg5200.txt	0
actual@pg2701.txt	0
actual@pg3825.txt	0
actual@pg74.txt	0
actual@pg55.txt	0
actual@pg1661.txt	0
actual@pg76.txt	0
actual@pg16328.txt	0
actual@pg103.txt	0
actual@pg1952.txt	0
actual@pg100.txt	0
actual@pg174.txt	0
actual@pg844.txt	0
actual@pg4300.txt	0
actual@pg2147.txt	0
actual@pg158.txt	0
actual@pg98.txt	0
actual@pg11.txt	0

----------------------------------------------------------------------
Bugs:
http，thish ，webby ，tremblin which in query.txt are not in output.txt,
because when processing the string 'http://www.gutenberg.org/1/11/', 
'tmp.replaceAll("[\\pP‘’“”=]", "")' will change the string to 'httpwwwgutenbergorg111',
so term 'http' can not be splited.  