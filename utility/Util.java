package utility;

import java.io.File;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Scanner;

import org.apache.hadoop.io.Text;

import distance.ContextObject;
import distance.DistanceMeasure;




/**
 * Util methods.
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * @author Francesco Gaetano - email: f.gaetano90@gmail.com
 * @author Luigi Lomasto - email: luigilomasto@gmail.com 
 * 
 * @version 1.7
 * 
 * Date: February, 20 2015
 */
public class Util {

	public static String[] tokenize(String text) {
		text = text.toLowerCase();
		text = text.replace("'","");
		text = text.replaceAll("[\\s\\W]+", " ").trim();
		return text.split(" ");
	}


	public static void printDistanceMatrix(double[][] matrix, ArrayList<String> seqs){
		NumberFormat formatter = new DecimalFormat("#0.0000000");
		System.out.print("\t");
		for(int j=0; j<seqs.size(); j++)
			System.out.print(seqs.get(j)+"\t");
		System.out.println();
		for(int i=0; i<seqs.size(); i++){
			System.out.print(seqs.get(i)+" ");
			for(int j=0; j<seqs.size(); j++){
				System.out.print(formatter.format(matrix[i][j])+"\t");
			}
			System.out.println();
		}
	} 


	public static String extractSpacedWord(String kmer, String pattern) {

		String spacedWord = "";
		for(int i=0; i<kmer.length(); i++)
			if(pattern.charAt(i)=='0')
				spacedWord+= Constant.JOLLY_CHARACTER;
			else
				spacedWord+= kmer.charAt(i);

		return spacedWord;
	}

	public static String extractPattern(String spacedWord) {

		String pattern = "";
		for(int i=0; i<spacedWord.length(); i++)
			if(spacedWord.charAt(i)==Constant.JOLLY_CHARACTER)
				pattern+= '0';
			else
				pattern+= '1';

		return pattern;
	}

	public static void printDistances(String file) {

		Scanner in = null;

		try{
			NumberFormat formatter = new DecimalFormat("#0.0000000");
			System.out.println("\nHadoop Distances:");

			in = new Scanner(new File(file));

			while (in.hasNextLine()){
				String[] s = in.nextLine().split("[ \t]");
				System.out.println("Pattern "+s[2]+" d_"+s[3].replaceAll("distance.", "")+"("+s[0]+","+s[1]+")="+formatter.format(Double.parseDouble(s[4])).replace(",", "."));
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		if(in!=null)
			in.close();

	}

	public static boolean deleteTree(String myFile) throws Exception{
		return deleteTree(new File(myFile));
	}

	public static boolean deleteTree(File myFile) throws Exception{

		if(myFile==null || myFile.getPath().equals("") || myFile.getPath().equals("/"))
			return false;


		if(myFile.isDirectory()){
			//DIRECTORY
			String[] filenames = myFile.list();
			for(int i=0; i<filenames.length; i++)
				if(!Util.deleteTree(new File(myFile, filenames[i])))
					return false;
		}

		//FILE o DIRECTORY VUOTA
		return myFile.delete();
	}


	public static boolean isValidFASTAFormat(String strGenome) { 

		if(Constant.CHECK_INPUT){
			//TODO return true/false.
			return true;
		}
		else
			return true;
	} 
	
	
	public static boolean isValidFASTAFormat(Text strGenome) {
		return Util.isValidFASTAFormat(strGenome.toString());
	} 

	public static String[] extractCurrentCO(String kmer, String pattern){
		String [] CO = new String[2];
		String context="";
		String object="";
		for(int i=0; i<kmer.length();i++){
			if(pattern.charAt(i)=='0'){
				//CO[1]=kmer.charAt(i)+"";
				object+=kmer.charAt(i);
				context+=Constant.JOLLY_CHARACTER;
			}
			else
				context+=kmer.charAt(i);
			
		}
		CO[0]=context;
		CO[1]=object;
		return CO;
	}
	
	public static StringBuilder reverseComplement(StringBuilder seq){
		int l = seq.length();
		StringBuilder reversed = new StringBuilder();
		char curr=0,cRev=0;
		//System.out.println("Original: "+seq);
		for(int i=l-1; i>=0; i--){
			 curr = seq.charAt(i);
			 cRev = complement(curr);
			 reversed.append(cRev);
					
		}
		//System.out.println("Reversed: "+reversed);
		return reversed;
		
	}
	
	
	public static char complement(char c){
		
		char cRev=0;
		if(c=='A')
			cRev='T';
		else if(c=='T')
			cRev='A';
		else if(c=='G')
			cRev='C';
		else if(c=='C')
			cRev='G';
		return cRev;
			
	}
	
	public static boolean isContexObjectMeasure(DistanceMeasure[] distances){
		boolean there_is_CP=false;
		for(DistanceMeasure d: distances){
			if(d instanceof ContextObject)
				there_is_CP = true;
					
		}
		return there_is_CP;
	}
	
	public static boolean isValidSpacedWordPattern(String pattern){
		if(pattern.startsWith("1") && pattern.endsWith("1"))
			return true;
		else
			return false;
	}
	
	public static boolean isCompatibile(String pattern){
		if(pattern.contains("0"))
			return true;
		else
			return false;
	}
	
}
