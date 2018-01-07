package cs.bigdata.Lab2.ParisTree;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;


public class ParisTreeReducer extends Reducer<Text, Text, Text, Text> {

	private IntWritable totalWordCount = new IntWritable();

	@Override
	public void reduce(final Text key, final Iterable<Text> values,
			final Context context) throws IOException, InterruptedException {

		double output[] = {0, 0};
		Iterator<Text> iterator = values.iterator();

		for(Text value : values) {
			String valueString = value.toString();
			String[] split = valueString.split("@");
			output[0] += Double.parseDouble(split[0]); // +1 arbre
			if(Double.parseDouble(split[1])>output[1]) { // Si on a un arbre plus grand
				output[1] = Double.parseDouble(split[1]);
			}

		}

		String outputString = String.valueOf((int) output[0]) + "\t" + String.valueOf(output[1]);
		context.write(key, new Text(outputString));
	}
}
