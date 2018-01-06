package cs.bigdata.Lab2.TfIdf;


import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;

// To complete according to your problem
public class PerWordDCMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
// Overriding of the map method
@Override
protected void map(LongWritable keyE, Text valE, Context context) throws IOException,InterruptedException
    {
        String[] keyValueSplit = valE.toString().split("\t"); // Séparation word@file -- a/n
        String[] wordFileSplit = keyValueSplit[0].split("@"); // Séparation word -- file                 //3/1500
        context.write(new Text(wordFileSplit[0]), new Text(wordFileSplit[1] + "=" + keyValueSplit[1])); // De la forme (word, filename=a/n)
    }
    

public void run(Context context) throws IOException, InterruptedException {
    setup(context);
    while (context.nextKeyValue()) {
        map(context.getCurrentKey(), context.getCurrentValue(), context);
    }
    cleanup(context);
}

}






