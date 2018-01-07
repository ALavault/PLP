package cs.bigdata.Tutorial2;


import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;



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
			// skip irrelevant data
			for(int i=1; i<22; i++){
				br.readLine();
			}
			line = br.readLine();

			int i = 0;
			while (line !=null){
				// Process of the current line
				// go to the next line

				String usaf = line.substring(0,7);
				String name = line.substring(13,43);
				String FIPS = line.substring(43,48);
				String altitude = line.substring(74,82);
				System.out.print(usaf+" ");
				System.out.print(name+" ");
				System.out.print(FIPS+" ");
				System.out.println(altitude);
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

