package cs.bigdata.Lab2.PageRank;


import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;

// To complete according to your problem
public class PageRankSortMapper extends Mapper<LongWritable, Text, Text, Text> {

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

			String child = tabIndex[1];
			String pagerank = child.split("@")[0];
			context.write(new Text(parent), new Text(pagerank));
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






