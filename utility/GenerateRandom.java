package utility;

import java.io.File;
import java.io.PrintStream;
import java.util.Random;

/**
 * Generating random file/s FASTA for dna and protein.
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * 
 * @version 1.2
 * 
 * Date: January, 26 2015
 */
public class GenerateRandom {

	
	public static String randomDNAString(int length) {
		return randomString(Constant.DNA_ALPHABET, length);
	}

	public static String randomProteinString(int length) {
		return randomString(Constant.PROTEIN_ALPHABET, length);
	}

	private static String randomString(String alphabet, int length) {

		Random rand = new Random(System.currentTimeMillis());

		StringBuilder randomString = new StringBuilder();

		for(int i=0;i<length;i++)	    	
			randomString.append(alphabet.charAt(rand.nextInt(alphabet.length())));

		return randomString.toString();
	}

	public static boolean randomDnaFastaFile(int numSeq, int avgSeqLength, int sdSeqLength, File f) {
		return randomFastaFile(Constant.DNA_ALPHABET, numSeq, avgSeqLength, sdSeqLength, f);
	}

	public static boolean randomDnaFastaFile(int numSeq, int avgSeqLength, File f) {
		return randomFastaFile(Constant.DNA_ALPHABET, numSeq, avgSeqLength, 0, f);
	}

	public static boolean randomProteinFastaFile(int numSeq, int avgSeqLength, File f) {
		return randomFastaFile(Constant.PROTEIN_ALPHABET, numSeq, avgSeqLength, 0, f);
	}

	public static boolean randomProteinFastaFile(int numSeq, int avgSeqLength, int sdSeqLength, File f) {
		return randomFastaFile(Constant.PROTEIN_ALPHABET, numSeq, avgSeqLength, sdSeqLength, f);
	}

	public static boolean randomFastaFile(String alphabet, int numSeq, int avgSeqLength, int sdSeqLength, File f) {

		PrintStream ps = null;
		boolean result = true;
		Random rand = new Random(System.currentTimeMillis());
		Random randSeq = new Random(System.currentTimeMillis());

		if(f==null || f.getPath().equals("") || f.getPath().equals("/"))
			return false;
		

		if(f.isDirectory()){
			File[] files = f.listFiles();
			for(File file : files)
				file.delete();
		}

		try {
			if(f.exists()==false || f.isFile())
				ps = new PrintStream(f);

			for(int i=1; i<=numSeq; i++){

				if(f.isDirectory())
					ps = new PrintStream(new File(f+"/Seq"+i+".fasta"));

				ps.println(">Seq"+i);

				int currSeqLength = Math.abs((int) Math.round(randSeq.nextGaussian() * sdSeqLength + avgSeqLength));
				//int currSeqLength = avgSeqLength;

				int j;
				String str;

				for(j=1;j<=(currSeqLength/Constant.FASTA_MAX_LINE_LENGTH);j++){	 
					str="";
					for(int k=1;k<=80;k++)
						str+=alphabet.charAt(rand.nextInt(alphabet.length()));
					ps.println(str);
				}

				int rest = currSeqLength - (Constant.FASTA_MAX_LINE_LENGTH*(j-1));
				str="";
				for(int k=1;k<=rest;k++)	 
					str+=alphabet.charAt(rand.nextInt(alphabet.length()));
				ps.println(str);


				if(f.isDirectory()){
					ps.close();
					ps=null;
				}
			}


		} catch (Exception e) {
			e.printStackTrace();
			result = false;
		}


		if(ps!=null)
			ps.close();

		return result;
	}


}
