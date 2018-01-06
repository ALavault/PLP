package cs.bigdata.Lab2.TfIdf;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

import java.util.Hashtable;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;


public class PerDocWCReducer extends Reducer<Text, Text, Text, Text > {

    @Override
    public void reduce(final Text keyE, final Iterable<Text> valE, final Context context) throws IOException, InterruptedException {
    	
        int docLength = 0;

        /*
        Map<String, Integer> tempCounter = new HashMap<String, Integer>();
        for (Text val : valE) {
            String[] wordCounter = val.toString().split("=");
            tempCounter.put(wordCounter[0], Integer.valueOf(wordCounter[1]));
            docLength += Integer.parseInt(val.toString().split("=")[1]);
        }
        for (String wordKey : tempCounter.keySet()) {
            context.write(new Text(wordKey + "@" + keyE.toString()), new Text(tempCounter.get(wordKey) + "/" + docLength));
        	
        }
        */
        Hashtable<String, Integer> tmpWordCounter = new Hashtable(); // Structure de dictionnaire, garantit l'unicité des clés
        for (Text value : valE) { // On itère sur les différentes valeurs d'une clé
            String[] valueSplit = value.toString().split("="); // Rappel (key, value) ~ (docid, word=wordcount)
            tmpWordCounter.put(valueSplit[0], Integer.valueOf(valueSplit[1])); // Split renvoie des String, donc conversion vers int nécessaire
            docLength += Integer.valueOf(valueSplit[1]);


        }
        for (String wordKey : tmpWordCounter.keySet()) {
            context.write(new Text(wordKey + "@" + keyE.toString()), new Text(tmpWordCounter.get(wordKey) + "/" + docLength));
        }
        //context.write(keyE, valE);

    }
}

