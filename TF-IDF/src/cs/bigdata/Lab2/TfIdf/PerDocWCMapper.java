package cs.bigdata.Lab2.TfIdf;


import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;

// To complete according to your problem
public class PerDocWCMapper extends Mapper<LongWritable, Text, Text, Text> {
	
@Override
public void map(LongWritable keyE, Text valE, Context context) throws IOException, InterruptedException {
	// keyE ~ Nombre -> Pas pertinent pour le problème
    String[] splitedPair = valE.toString().split("\t"); // une tabulation sert de séparateur entre "clé" et valeur
    //System.out.println(splitedPair[0]);

    String[] extractedKeys = splitedPair[0].split("@"); // mot@filename et split-> [mot, filename]
    //System.out.println(extractedKeys[0]);
    context.write(new Text(extractedKeys[1]), new Text(extractedKeys[0] + "=" + splitedPair[1])); // (docid, word=wordcount)
}

public void run(Context context) throws IOException, InterruptedException {
    setup(context);
    while (context.nextKeyValue()) {
        map(context.getCurrentKey(), context.getCurrentValue(), context);
    }
    cleanup(context);
}

}






