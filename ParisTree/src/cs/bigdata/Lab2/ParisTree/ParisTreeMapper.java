package cs.bigdata.Lab2.ParisTree;


import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;

// To complete according to your problem
public class ParisTreeMapper extends Mapper<LongWritable, Text, Text, Text> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	// Overriding of the map method
	@Override
	protected void map(LongWritable keyE, Text valE, Context context) throws IOException,InterruptedException
	{
		// To complete according to the processing
		// Processing : keyI = ..., valI = ...
		String line = valE.toString();
		if(valE.charAt(0) == '('){ // On évacue le header du CSV
			String[] split = line.split(";");
			// Les informations sont en positions 
			String species = split[2];
			// La hauteur peut être vide -> on la remplace par 0 dans ce cas
			String heightString =  split[6];
			float height = 0f;
			if(heightString.length()==0){
				height = 0f;
				heightString = "0.0";			}
			else {
				height = Float.parseFloat(heightString);
			}
			String output = "";
			output += "1@";
			output += heightString;
			context.write(new Text(species), new Text(output));
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






