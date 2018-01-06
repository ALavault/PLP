package cs.bigdata.Tutorial2;


import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;




public class CompterLigneFile extends Configured implements Tool{
	
	public int run(String[] args) throws IOException{

		String localSrc = "/home/cloudera/Downloads/arbres.csv";
		//Open the file
		Configuration conf = new Configuration();
        FileSystem fs = FileSystem.newInstance(getConf());
        
        Job job = Job.getInstance(getConf());
        job.setJobName("WordCount");
		//FileSystem fs = FileSystem.get(conf);
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		int count = 0;

		try{
			InputStreamReader isr = new InputStreamReader(in);
			BufferedReader br = new BufferedReader(isr);

			// read line by line
			String line = br.readLine();
			while (line !=null){
				// Process of the current line
				count ++;
				// go to the next line
				line = br.readLine();
			}
		}
		finally{
			//close the file
			in.close();
			fs.close();
		}
		return count;

	}

	public static void main(String[] args) throws Exception {

		CompterLigneFile exempleDriver = new CompterLigneFile();

		int res = ToolRunner.run(exempleDriver, args);
		System.out.print(res);
		System.exit(res);
		



	}	



}



