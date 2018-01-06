package cs.bigdata.Lab2.PageRank;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;


public class PageRankSortReducer extends Reducer<Text, Text, Text, Text> {

	private IntWritable totalWordCount = new IntWritable();

	@Override
	public void reduce(final Text key, final Iterable<Text> values,
			final Context context) throws IOException, InterruptedException {

		String value = values.toString();
		String output = "";
		for(Text value1 : values) {
			output += value1.toString();
		}

		context.write(key, new Text(output));
	}
}