package cs.bigdata.Lab2.TfIdf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Hashtable;


public class PerWordDCReducer extends Reducer<Text, Text, Text, Text> {

	 @Override
	 protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	        // Le nombre de document est transmis via le contexte
			Configuration conf = context.getConfiguration();
			String strProp = conf.get("numberOfDocs");
			Integer numberOfDoc = Integer.valueOf(strProp);
	        int docNumberWithKey = 0;
	        Hashtable<String, String> tmpFreq = new Hashtable<String, String>();
	        for (Text val : values) {
	            String[] docAndFreq = val.toString().split("=");
	            docNumberWithKey++;
	            tmpFreq.put(docAndFreq[0], docAndFreq[1]);
	        }
	        for (String document : tmpFreq.keySet()) {
	            String[] wordFreqTotalSplit = tmpFreq.get(document).split("/");
	 
	            double tf = Double.valueOf(Double.valueOf(wordFreqTotalSplit[0])/ Double.valueOf(wordFreqTotalSplit[1])); //tf ~ term frequency
	 
	            double idf = (double) numberOfDoc / (double) docNumberWithKey;//idf ~ inverse document frequency
	 
	            double tfIdf = tf*Math.log(idf);
	 
	            context.write(new Text(key + "@" + document), new Text(String.format("%.10f", tfIdf)));
	        }
	 }
}
