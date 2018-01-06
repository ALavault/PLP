package cs.bigdata.Tutorial2;


import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;


public final class Tree {
	private Tree () { // private constructor
		GEOPOINT = "";
		ARRONDISSEMENT = "";
		GENRE = "";
		ESPECE= "";
		FAMILLE = "";
		ANNEE_PLANTATION= "";
		HAUTEUR = "";
		CIRCONFERENCE= "";
		ADRESSE= "";
		NOM_COMMUN="";
		VARIETE= "";
		OBJECTID= "";
		NOM_EV= "";
	}
	private static String GEOPOINT;
	private static String ARRONDISSEMENT;
	private static String GENRE;
	private static String ESPECE;
	private static String FAMILLE;
	private static String ANNEE_PLANTATION;
	private static String HAUTEUR;
	private static String CIRCONFERENCE;
	private static String ADRESSE;
	private static String NOM_COMMUN;
	private static String VARIETE;
	private static String OBJECTID;
	private static String NOM_EV;
	
	static void setTree(String line){
		String[] parsed = parser(line);
		GEOPOINT = parsed[0];
		ARRONDISSEMENT = parsed[1];
		GENRE = parsed[2];
		ESPECE= parsed[3];
		FAMILLE = parsed[4];
		ANNEE_PLANTATION= parsed[5];
		HAUTEUR = parsed[6];
		CIRCONFERENCE= parsed[7];
		ADRESSE= parsed[8];
		NOM_COMMUN= parsed[9];
		VARIETE= parsed[10];
		OBJECTID= parsed[11];
		NOM_EV= parsed[12];
	}


	private static String[] parser(String line){
		int n = line.length();
		int idx = 0;
		String[] parsed = new String[13];
		String part ="";
		for (int i=0; i<n; i++){
			if (line.charAt(i) == ';'){ // Si on atteint un séparateur
				parsed[idx] = part;
				idx++;
				part = "";
			}
			else{
				part += line.charAt(i);
			}
		}

		return parsed;
	}
	static float[] getYearHeight(){
		float Year;
		float Height;
		if(ANNEE_PLANTATION.length() == 0){
			Year = Float.NaN;
			}
		else{
			Year = Float.parseFloat(ANNEE_PLANTATION);
		}
		if(HAUTEUR.length() == 0){
			Height = Float.NaN;
			}
		else{
			Height = Float.parseFloat(HAUTEUR);
		}
		float[] result = new float[2];
		result[0] = Year;
		result[1] = Height;
		return result;

	}

}