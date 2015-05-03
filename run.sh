#!/bin/bash
echo "compile"
javac -classpath hadoop-1.2.1/hadoop-core-1.2.1.jar:hadoop-1.2.1/lib/commons-cli-1.2.jar -d tfidf_classes TFIDF.java 
echo "generate jar"
jar cvf tfidf.jar -C tfidf_classes/ .
echo "remove output folder"
hadoop dfs -rmr output
echo "run hadoop job"
hadoop jar tfidf.jar TFIDF books output
echo "show result"
hadoop dfs -cat output2/part-r-00000 > output2.txt
head -200 output2.txt 
