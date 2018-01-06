#!/bin/bash
hadoop fs -mkdir input
wget http://www.textfiles.com/etext/FICTION/defoe-robinson-103.txt
hadoop fs -copyFromLocal defoe-robinson-103.txt input
wget http://www.textfiles.com/etext/FICTION/callwild
hadoop fs -copyFromLocal callwild input
hadoop jar tfidf.jar cs.bigdata.Lab2.TfIdf.MainDriver input output
hadoop fs -cat output/part* | sort -g -k2 -r | head -n20

