package cs.bigdata.Lab2.PageRank;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;


public class PageRankProcessingReducer extends Reducer<Text, Text, Text, Text> {

	private IntWritable totalWordCount = new IntWritable();
	private final float DAMPING_FACTOR = (float) 0.85;

	@Override
	public void reduce(final Text key, final Iterable<Text> values,
			final Context context) throws IOException, InterruptedException {
		String outlink1 = ""; 
		float pagerank = 0;

		for(Text value1 : values) {
			String value = value1.toString();
			if (value.indexOf('@')>-1){
				outlink1 = value.split("@")[1];
			}
			else {
				pagerank += Float.parseFloat(value);
			}
		}
		pagerank = 1 - DAMPING_FACTOR + ( DAMPING_FACTOR * pagerank );

		String outlink = String.valueOf(pagerank) + "@" + outlink1;
		context.write(key, new Text(outlink));
	}
}
