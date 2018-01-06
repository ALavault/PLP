package cs.bigdata.Lab2.PageRank;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;


public class PageRankReducer extends Reducer<Text, Text, Text, Text> {

    private IntWritable totalWordCount = new IntWritable();

    @Override
    public void reduce(final Text key, final Iterable<Text> values,
            final Context context) throws IOException, InterruptedException {

        boolean first = true;
        StringBuilder links = new StringBuilder();
        links.append("1@");
        for (Text value : values) {
            if (!first) 
                links.append(",");
            links.append(value.toString());
            first = false;
        }

context.write(key, new Text(links.toString()));
    }
}
