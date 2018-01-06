package cs.bigdata.Lab2.TfIdf;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import java.io.IOException;
import java.math.*;
import java.util.Hashtable;
import java.util.Iterator;


public class PerWordDCReducer extends Reducer<Text, Text, Text, Text> {

	 @Override
	 protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	        // get the number of documents indirectly from the file-system (stored in the job name on purpose)
			Configuration conf = context.getConfiguration();
			String strProp = conf.get("numberOfDocs");
			Integer numberOfDocumentsInCorpus = Integer.valueOf(strProp);
			// total frequency of this word
	        int numberOfDocumentsInCorpusWhereKeyAppears = 0;
	        Hashtable<String, String> tempFrequencies = new Hashtable<String, String>();
	        for (Text val : values) {
	            String[] documentAndFrequencies = val.toString().split("=");
	            numberOfDocumentsInCorpusWhereKeyAppears++;
	            tempFrequencies.put(documentAndFrequencies[0], documentAndFrequencies[1]);
	        }
	        for (String document : tempFrequencies.keySet()) {
	            String[] wordFrequenceAndTotalWords = tempFrequencies.get(document).split("/");
	 
	            double tf = Double.valueOf(Double.valueOf(wordFrequenceAndTotalWords[0])/ Double.valueOf(wordFrequenceAndTotalWords[1])); //tf ~ term frequency
	 
	            double idf = (double) numberOfDocumentsInCorpus / (double) numberOfDocumentsInCorpusWhereKeyAppears;//idf ~ inverse document frequency
	 
	            double tfIdf = tf*Math.log(idf);
	 
	            context.write(new Text(key + "@" + document), new Text(String.format("%.10f", tfIdf)));
	        }
	 }
}
