package cs.bigdata.Lab2.TfIdf;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.util.Tool;

import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.io.IntWritable;


public class MainDriver extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: [input] [output]");
            System.exit(-1);
        }
        
        // Création d'un job en lui fournissant la configuration et une description textuelle de la tâche

        Job job1 = Job.getInstance(getConf());

        job1.setJobName("TF-IDF Round 1");


        // On précise les classes MyProgram, Map et Reduce

        //job1.setJarByClass(WordCount.class);
        job1.setJarByClass(WordCount.class);


        job1.setMapperClass(WordCountMapper.class);

        job1.setReducerClass(WordCountReducer.class);


        // Définition des types clé/valeur de notre problème

        job1.setMapOutputKeyClass(Text.class);

        job1.setMapOutputValueClass(IntWritable.class);


        job1.setOutputKeyClass(TextInputFormat.class);

        job1.setOutputValueClass(TextOutputFormat.class);

        Path inputFilePath = new Path(args[0]);
        Path outputFilePath = new Path("output1");
        
        // Nombre de documents dans le corpus
        int docCount = 0;
        FileSystem fs_ = FileSystem.get(getConf());
        boolean recursive = false;
        RemoteIterator<LocatedFileStatus> ri = fs_.listFiles(inputFilePath, recursive);
        while (ri.hasNext()){
            docCount++;
            ri.next();
        }

     // On accepte une entree recursive
        FileInputFormat.setInputDirRecursive(job1, true);

        FileInputFormat.addInputPath(job1, inputFilePath);
        FileOutputFormat.setOutputPath(job1, outputFilePath);

        FileSystem fs = FileSystem.newInstance(getConf());

        if (fs.exists(outputFilePath)) {
            fs.delete(outputFilePath, true);
        }

        job1.waitForCompletion(true);
        
        ////////////////////////////////
        /// Job 2       ////////////////
        ////////////////////////////////
        
 
        // Création d'un job en lui fournissant la configuration et une description textuelle de la tâche

        Job job2 = Job.getInstance(getConf());

        job2.setJobName("WordCountPerDoc");


        // On précise les classes MyProgram, Map et Reduce

        job2.setJarByClass(PerDocWC.class);

        job2.setMapperClass(PerDocWCMapper.class);

        job2.setReducerClass(PerDocWCReducer.class);


        // Définition des types clé/valeur de notre problème

        job2.setMapOutputKeyClass(Text.class);

        job2.setMapOutputValueClass(Text.class);


        job2.setOutputKeyClass(TextInputFormat.class);

        job2.setOutputValueClass(TextOutputFormat.class);

        Path inputFilePath1 = outputFilePath;
        Path outputFilePath1 = new Path("output2");
        

     // On accepte une entree recursive
        FileInputFormat.setInputDirRecursive(job2, true);

        FileInputFormat.addInputPath(job2, inputFilePath1);
        FileOutputFormat.setOutputPath(job2, outputFilePath1);

        FileSystem fs1 = FileSystem.newInstance(getConf());

        if (fs1.exists(outputFilePath1)) {
            fs1.delete(outputFilePath1, true);
        }
        
        job2.waitForCompletion(true);
        
        ////////////////////////////////
        /// Job 3///////////////////////
        ////////////////////////////////

        Configuration conf = getConf();
        conf = getConf();
        conf.set("numberOfDocs", String.valueOf(docCount));

        // Création d'un job en lui fournissant la configuration et une description textuelle de la tâche

        Job job3 = Job.getInstance(getConf());


        job3.setJobName("Doc Count");


        // On précise les classes MyProgram, Map et Reduce

        job3.setJarByClass(PerWordDC.class);

        job3.setMapperClass(PerWordDCMapper.class);

        job3.setReducerClass(PerWordDCReducer.class);


        // Définition des types clé/valeur de notre problème

        job3.setMapOutputKeyClass(Text.class);

        job3.setMapOutputValueClass(Text.class);


        job3.setOutputKeyClass(TextInputFormat.class);

        job3.setOutputValueClass(TextOutputFormat.class);

        Path inputFilePath11 = outputFilePath1;
        Path outputFilePath11 = new Path(args[1]);
        

     // On accepte une entree recursive
        FileInputFormat.setInputDirRecursive(job3, true);

        FileInputFormat.addInputPath(job3, inputFilePath11);
        FileOutputFormat.setOutputPath(job3, outputFilePath11);

        FileSystem fs2 = FileSystem.newInstance(getConf());

        if (fs2.exists(outputFilePath11)) {
            fs2.delete(outputFilePath11, true);
        }
        

        
        
        
        
		return job3.waitForCompletion(true) ? 0: 1;
        
        
        
    }


    public static void main(String[] args) throws Exception {

        MainDriver exempleDriver = new MainDriver();

        int res = ToolRunner.run(exempleDriver, args);

        System.exit(res);

    }

}

