package cs.bigdata.Lab2.PageRank;


import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;

// To complete according to your problem
public class PageRankProcessingMapper extends Mapper<LongWritable, Text, Text, Text> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	// Overriding of the map method
	@Override
	protected void map(LongWritable keyE, Text valE, Context context) throws IOException,InterruptedException
	{
		// Si on n'a pas un commentaire..
		if (valE.charAt(0) != '#') {
			String[] tabIndex = valE.toString().split("\t");
			String parent = tabIndex[0];
			String values = tabIndex[1];

			String[] valueSplit = values.split("@");
			String value = valueSplit[0].toString();
			float pagerank = Float.parseFloat(value);
			if(valueSplit.length>1){
				String[] neighbours = valueSplit[1].split(",");
				int arity = neighbours.length;

				for(String outLink : neighbours) {
					context.write(new Text(outLink), new Text(String.valueOf(pagerank/(float)arity)));
				}

				values = pagerank+"@"+valueSplit[1];
				context.write(new Text(parent), new Text(values));
			}

		}
	}

	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		while (context.nextKeyValue()) {
			map(context.getCurrentKey(), context.getCurrentValue(), context);
		}
		cleanup(context);
	}
}






