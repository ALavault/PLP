package cs.bigdata.Lab2.PageRank;

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


public class PageRank extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: [input] [output]");
			System.exit(-1);
		}

		int ITERATIONS = 3;
		// Création d'un job en lui fournissant la configuration et une description textuelle de la tâche

		Job job = Job.getInstance(getConf());

		job.setJobName("PageRank");


		// On précise les classes MyProgram, Map et Reduce

		job.setJarByClass(PageRank.class);

		job.setMapperClass(PageRankMapper.class);

		job.setReducerClass(PageRankReducer.class);


		// Définition des types clé/valeur de notre problème

		job.setMapOutputKeyClass(Text.class);

		job.setMapOutputValueClass(Text.class);


		job.setOutputKeyClass(TextInputFormat.class);

		job.setOutputValueClass(TextOutputFormat.class);

		Path inputFilePath = new Path(args[0]);
		Path outputFilePath = new Path("output1");


		// On accepte une entree recursive
		FileInputFormat.setInputDirRecursive(job, true);

		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);

		FileSystem fs = FileSystem.newInstance(getConf());

		if (fs.exists(outputFilePath)) {
			fs.delete(outputFilePath, true);
		}

		job.waitForCompletion(true);

		//////////////////////////
		//// Calcul effectif /////
		////////////////////////// 
		int i=1;
		while(i<ITERATIONS) {
			Job job1 = Job.getInstance(getConf());

			job1.setJobName("PageRank Iteration");


			// On précise les classes MyProgram, Map et Reduce

			job1.setJarByClass(PageRank.class);

			job1.setMapperClass(PageRankProcessingMapper.class);

			job1.setReducerClass(PageRankProcessingReducer.class);


			// Définition des types clé/valeur de notre problème

			job1.setMapOutputKeyClass(Text.class);

			job1.setMapOutputValueClass(Text.class);


			job1.setOutputKeyClass(TextInputFormat.class);

			job1.setOutputValueClass(TextOutputFormat.class);

			
			// Limite la place prise par les traitements intermédiaires -> alternance
			String inputString = "output" + String.valueOf(i%2);
			String outputString = "output" + String.valueOf((i+1)%2);


			Path inputFilePath1 = new Path(inputString);
			Path outputFilePath1 = new Path(outputString);


			// On accepte une entree recursive
			FileInputFormat.setInputDirRecursive(job1, true);

			FileInputFormat.addInputPath(job1, inputFilePath1);
			FileOutputFormat.setOutputPath(job1, outputFilePath1);

			FileSystem fs1 = FileSystem.newInstance(getConf());

			if (fs1.exists(outputFilePath1)) {
				fs1.delete(outputFilePath1, true);
			}

			job1.waitForCompletion(true);
			i++;
		}
		
		
		///////////
		// Tri ///
		/////////
		
		Job job2 = Job.getInstance(getConf());

		job2.setJobName("PageRank Final Stage");


		// On précise les classes MyProgram, Map et Reduce

		job2.setJarByClass(PageRank.class);

		job2.setMapperClass(PageRankSortMapper.class);

		job2.setReducerClass(PageRankSortReducer.class);


		// Définition des types clé/valeur de notre problème

		job2.setMapOutputKeyClass(Text.class);

		job2.setMapOutputValueClass(Text.class);


		job2.setOutputKeyClass(TextInputFormat.class);

		job2.setOutputValueClass(TextOutputFormat.class);

		Path inputFilePath1 = new Path("output" + String.valueOf(i%2));
		Path outputFilePath1 = new Path(args[1]);


		// On accepte une entree recursive
		FileInputFormat.setInputDirRecursive(job, true);

		FileInputFormat.addInputPath(job2, inputFilePath1);
		FileOutputFormat.setOutputPath(job2, outputFilePath1);

		FileSystem fs1 = FileSystem.newInstance(getConf());

		if (fs1.exists(outputFilePath1)) {
			fs1.delete(outputFilePath1, true);
		}

		

		return job2.waitForCompletion(true)? 0: 1;



	}


	public static void main(String[] args) throws Exception {

		PageRank exempleDriver = new PageRank();

		int res = ToolRunner.run(exempleDriver, args);

		System.exit(res);

	}

}

