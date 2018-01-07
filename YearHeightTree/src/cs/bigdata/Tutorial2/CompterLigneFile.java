package cs.bigdata.Tutorial2;


import java.io.*;

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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;



public class CompterLigneFile {

	public static void main(String[] args) throws IOException {
	        if (args.length != 1) {
	            System.out.println("Usage: [input]");
	            System.exit(-1);
	        }
		String localSrc = args[0];
		Path src = new Path(localSrc);
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		//InputStream in = new BufferedInputStream(new FileInputStream(src));
		FSDataInputStream in = fs.open(src);
		try{
			InputStreamReader isr = new InputStreamReader(in);
			BufferedReader br = new BufferedReader(isr);

			// read line by line
			String line = br.readLine();
			line = br.readLine();
			
			
			


			while (line !=null){
				// Process of the current line
				// go to the next line
				Tree.setTree(line);
				float[] result = Tree.getYearHeight();
				System.out.print((int)result[0]);
				System.out.print(" ");
				System.out.println(result[1]);
				line = br.readLine();

				


			}
		}
		finally{
			//close the file
			in.close();
			fs.close();
		}

	}	
	
}

