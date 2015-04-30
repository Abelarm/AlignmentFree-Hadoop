package sequential;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import distance.DistanceMeasure;
import distance.Parameters;
import utility.Constant;
import utility.Util;

/**
 * Compute k-tuple distances between DNA sequences.
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * 
 * @version 4.0
 * 
 * Date: February, 1 2015
 */
public class ComputeDistancesSequentialDriverFastD2Star {

	private String outputDir;
	private String inputFiles;
	private String patternFile;
	private String probabilitiesFile;
	private Class<DistanceMeasure>[] distClasses;

	public ComputeDistancesSequentialDriverFastD2Star(String inputFiles, String patternFile, String probabilitiesFile, String outputDir, Class<DistanceMeasure>[] distClasses){
		this.inputFiles = inputFiles;
		this.patternFile = patternFile;
		this.probabilitiesFile = probabilitiesFile;
		this.outputDir = outputDir;
		this.distClasses = distClasses;

	}

	public void start() {

		/*
		 * For comparison see URLs:
		 * - http://spaced.gobics.de/spaced.php (use Euclidean.class)
		 * - http://guanine.evolbio.mpg.de/aliFreeReview/kt_0.2.tgz (use SquaredEuclideanWithFrequencies.class)
		 */

		//Instanzio le distanze da usare.
		DistanceMeasure[] distances = new DistanceMeasure[distClasses.length];

		for(int i=0; i<distances.length; i++){
			try {
				Object obj = distClasses[i].newInstance();

				if(obj instanceof DistanceMeasure)
					distances[i] = (DistanceMeasure) obj;

			} catch (Exception e) {
				e.printStackTrace();
			} 
		}

		System.out.println("k-mers extraction (Sequential)");
		readFastaAndCompute(distances);

	}

	private void readFastaAndCompute(DistanceMeasure[] distances){

		List<SequenceCounts> infoSeqs = new ArrayList<SequenceCounts>();
		String name = "";
		Map<String, Integer> allKmersSeq;		
		PrintStream psIds = null;

		try {
			if(outputDir!=null && outputDir.equals("")==false && outputDir.equals("/")==false)
				new File(outputDir).delete();

			new File(outputDir).mkdirs();

			//			File outputDirFiles = new File(this.outputDir);
			//			if(!outputDirFiles.exists())
			//				outputDirFiles.mkdirs();

			psIds = new PrintStream(new File(this.outputDir+Constant.LOCAL_OUTPUT_IDS_SEQ));
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
			return;
		}


		List<String> patterns = readPatterns(patternFile);
		ArrayList<Map<String, Double>> probabilities = readProbabilities(probabilitiesFile);
		if(Constant.DEBUG_MODE){
			for(Map<String, Double> singleP : probabilities){
				if(singleP==null)
					continue;
				System.out.println("A "+singleP.get("A"));
				System.out.println("C "+singleP.get("C"));
				System.out.println("G "+singleP.get("G"));
				System.out.println("T "+singleP.get("T"));
				System.out.println("-------------------------------------------------------------------");
			}
		}

		File inputF = new File(inputFiles);
		File[] files;

		if(inputF.isDirectory())
			files = inputF.listFiles();
		else
			files = new File[]{inputF};

		for(File f: files){ //Elaboro ogni file di input

			Scanner in = null;
			try {
				in = new Scanner(f);

				String line;
				StringBuilder seq = new StringBuilder(Constant.BUFFER_CAPACITY_SEQ);

				while(in.hasNextLine()){

					line = in.nextLine();

					if(line.startsWith(">")){
						String oldName = name;
						name = line.substring(1);

						if(seq.length()>0){//processo il contenuto di seq se non e' vuoto
							//if(!seq.equals("")){//processo il contenuto di seq se non e' vuoto
							psIds.println(oldName + "\t"+ seq.length());
							allKmersSeq = kmersCountsExtract(seq, patterns); //estrae i kmers per tutti i patterns
							infoSeqs.add(new SequenceCounts(allKmersSeq, seq.length(), oldName)); 
							if(Constant.DEBUG_MODE){
								System.out.println("Counts: "+allKmersSeq+"\n");
							}
							//System.out.println(line);
							seq = new StringBuilder(Constant.BUFFER_CAPACITY_SEQ);
						}
						if(Constant.DEBUG_MODE){
							System.out.println(name);
						}
					}
					else
						seq.append(line);

				}

				psIds.println(name + "\t"+ seq.length());
				allKmersSeq = kmersCountsExtract(seq, patterns); //estrae i kmers per tutti i patterns
				infoSeqs.add(new SequenceCounts(allKmersSeq, seq.length(), name)); 

				//if(Constant.DEBUG_MODE)//TODO
				System.out.println("Counts: "+allKmersSeq+"\n");

			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			if(in!=null)
				in.close();

		}

		if(psIds!=null)
			psIds.close();

		computeDistances(distances, patterns, probabilities, infoSeqs);

	}

	private static ArrayList<Map<String, Double>> readProbabilities(String probFile) {
		
		ArrayList<Map<String, Double>> listaProb = new ArrayList<Map<String, Double>>();
		HashMap<String, Double> prob=null;
		
		try {
			Scanner in = new Scanner(new File(probFile));
			String currLine;
			int currSeqNum=0;
			while(in.hasNextLine()){
				currLine = in.nextLine();
				if(currLine.contains(">Seq")){
					listaProb.add(currSeqNum, prob);
					currSeqNum = Integer.parseInt(currLine.substring(4));
					prob = new HashMap<String, Double>();
				}
				else{
					String letter=currLine.substring(0, 1);
					double probability = Double.parseDouble(currLine.substring(2));
					prob.put(letter, probability);
				}
			}
			listaProb.add(currSeqNum, prob);
			in.close();

		} catch (Exception e) {
			//e.printStackTrace();
			//genero probabilità uniformi?
		}

		return listaProb;
	}
	
	
//	public void testFasta(int k){
//
//		PrintStream ps1 = null;
//		PrintStream ps2 = null;
//
//		try {
//			if(outputDir!=null && outputDir.equals("")==false && outputDir.equals("/")==false)
//				new File(outputDir).delete();
//
//			new File(outputDir).mkdirs();
//
//			ps1 = new PrintStream(new File(this.outputDir+"/Seq_File1.txt"));
//			ps2 = new PrintStream(new File(this.outputDir+"/Seq_File2.txt"));
//		} catch (FileNotFoundException e1) {
//			e1.printStackTrace();
//			return;
//		}
//
//		File inputF = new File(inputFiles);
//		File[] files;
//
//		if(inputF.isDirectory())
//			files = inputF.listFiles();
//		else
//			files = new File[]{inputF};
//
//		for(File f: files){ //Elaboro ogni file di input
//
//			Scanner in = null;
//			try {
//				in = new Scanner(f);
//
//				String line;
//
//				boolean first = true;
//
//				while(in.hasNextLine()){
//
//					line = in.nextLine();
//
//					if(line.startsWith(">") || line.length()==0)
//						continue;
//
//					ps1.println(line);
//
//					if(first)
//						first=false;
//					else
//						ps2.println(line.substring(0, Math.min(k, line.length())));
//
//				}
//
//
//			} catch (FileNotFoundException e) {
//				e.printStackTrace();
//			}
//			if(in!=null)
//				in.close();
//
//		}
//
//		if(ps1!=null)
//			ps1.close();
//
//		if(ps2!=null)
//			ps2.close();
//
//	}


	private void computeDistances(DistanceMeasure[] distances, List<String> patterns, ArrayList<Map<String, Double>> probabilities, List<SequenceCounts> infoSeqs) {

		//Misure di distanza
		NumberFormat formatter = new DecimalFormat("#0.0000000");
		double[][] matrix;
		int start;
		PrintStream psDistances = null;
		try {

			//			if(outputDir!=null && outputDir.equals("")==false && outputDir.equals("/")==false)
			//				new File(outputDir).delete();
			//
			//			new File(outputDir).mkdirs();

			psDistances = new PrintStream(new File(this.outputDir+Constant.LOCAL_OUTPUT_FILE_SEQ));

		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return;
		} 

		for(DistanceMeasure d : distances){

			for(String pattern : patterns){

				int k = pattern.length();

				if(Constant.PRINT_MATRIX_SEQ)
					matrix = new double[infoSeqs.size()][infoSeqs.size()];

				if(Constant.SEQUENTIAL_PRINT_DISTANCES){
					if(pattern.contains("0"))
						System.out.println("\n"+k+"-mers extractions with distance "+d.getName()+" and pattern "+pattern);
					else
						System.out.println("\n"+k+"-mers extractions with distance "+d.getName());
				}

				for(int i=0; i<infoSeqs.size(); i++){

					if(d.isSymmetricMeasure())
						start = i+1;
					else
						start = 0;

					for(int j=start; j<infoSeqs.size(); j++){
						double val;
						//if(d.getName().contains("Fixed"))
						SequenceCounts seq1 = infoSeqs.get(i);
						SequenceCounts seq2 = infoSeqs.get(j);
						Map<String, Double> prob1 = probabilities.get(Integer.parseInt(infoSeqs.get(i).getName().substring(3)));
						Map<String, Double> prob2 = probabilities.get(Integer.parseInt(infoSeqs.get(j).getName().substring(3)));
							val = computeDissimilarityMeasure(d, seq1, seq2, pattern, prob1, prob2);
						//else
						//	val = computeDissimilarityMeasure(d, infoSeqs.get(i), infoSeqs.get(j), pattern, null, null);
						if(Constant.PRINT_MATRIX_SEQ)
							if(d.isSymmetricMeasure())
								matrix[j][i] = matrix[i][j] = val;
							else
								matrix[i][j] = val;

						if(Constant.SEQUENTIAL_PRINT_DISTANCES){
							System.out.println("Pattern "+pattern+" d_"+d.getName()+"("+infoSeqs.get(i).getName()+","+infoSeqs.get(j).getName()+")="+formatter.format(val).replace(",", "."));
							//System.out.println("d("+infoSeqs.get(i).getName()+","+infoSeqs.get(j).getName()+")="+val);
						}

						if(psDistances!=null)
							psDistances.println("Pattern "+pattern+" d_"+d.getName()+"("+infoSeqs.get(i).getName()+","+infoSeqs.get(j).getName()+")="+val);
					}
				}


				if(Constant.PRINT_MATRIX_SEQ){
					if(pattern.contains("0"))
						System.out.println("\n"+k+"-mers extractions with distance "+d.getName()+" and pattern "+pattern);
					else
						System.out.println("\n"+k+"-mers extractions with distance "+d.getName());
					printDistanceMatrix(matrix, infoSeqs);
				}
			}
		}

		if(psDistances!=null)
			psDistances.close();

	}

	private static List<String> readPatterns(String filename) {

		Set<String> patterns = new HashSet<String>();

		try {
			Scanner in = new Scanner(new File(filename));

			while(in.hasNextLine())
				patterns.add(in.nextLine());

			in.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

		return new ArrayList<String>(patterns);
	}

	public static void printDistanceMatrix(double[][] matrix, List<SequenceCounts> seqs){
		NumberFormat formatter = new DecimalFormat("#0.0000000");
		System.out.println("\n"+matrix.length);
		for(int i=0; i<seqs.size(); i++){
			System.out.print(seqs.get(i).getName()+"\t");
			for(int j=0; j<seqs.size(); j++){
				System.out.print(formatter.format(matrix[i][j]).replace(",", ".")+" ");
			}
			System.out.println();
		}
	} 

	public static void printDistanceMatrixStd(double[][] matrix, List<SequenceCounts> seqs){
		NumberFormat formatter = new DecimalFormat("#0.0000000");
		System.out.print("\n\t");
		for(int j=0; j<seqs.size(); j++)
			System.out.print(seqs.get(j).getName()+"\t");
		System.out.println();
		for(int i=0; i<seqs.size(); i++){
			System.out.print(seqs.get(i).getName()+" ");
			for(int j=0; j<seqs.size(); j++){
				System.out.print(formatter.format(matrix[i][j]).replace(",", ".")+"\t");
			}//TODO riusare oggetti.
			System.out.println();
		}
	} 


	//Frequenze non normalizzate
	private static double computeDissimilarityMeasure(DistanceMeasure distance, SequenceCounts seq1, SequenceCounts seq2, String pattern, Map<String, Double> probabilities1, Map<String, Double> probabilities2) {//Usata per misure simmetriche

		double res, dist = distance.initDistance();
		Integer c1, c2;
		int k = pattern.length();

		Set<String> set1 = seq1.getAllCountKmers().keySet();
		Set<String> set2 = seq2.getAllCountKmers().keySet();

		Set<String> mySet = new HashSet<String>(); // The variable "mySet" does not support the add or addAll operations.
		mySet.addAll(set1);
		mySet.addAll(set2);
		//TODO riusare oggetti.
		Iterator<String> it = mySet.iterator();
		while(it.hasNext()){
			//TODO riusare oggetti.//TODO riusare oggetti.
			String kmerCurr = it.next();
			//System.out.println(kmerCurr);

			//Se il kmer in esame e' compatibile con il pattern allora procedo.
			if(!match(kmerCurr, pattern))
				continue;

			c1 = seq1.getAllCountKmers().get(kmerCurr);
			c2 = seq2.getAllCountKmers().get(kmerCurr);

			if(c1==null)
				c1=0;

			if(c2==null)
				c2=0;

			Parameters param = new Parameters();
			param.setC1(c1);
			param.setC2(c2);
			param.setLength1(seq1.getLength());
			param.setLength2(seq2.getLength());
			param.setK(k);
			param.setKmer(kmerCurr);
			param.setMapS1(seq1.getAllCountKmers());
			param.setMapS2(seq2.getAllCountKmers());
			param.setProbMap1(probabilities1);
			param.setProbMap2(probabilities2);
			param.setNumLettere(probabilities1.size());

			res = distance.computePartialDistance(param); 

			//res = distance.computePartialDistance(c1, seq1.getLength(), c2, seq2.getLength(), k);
			dist = distance.distanceOperator(dist, res);
		}

		dist = distance.finalizeDistance(dist,0);

		return dist;
	}

	private static boolean match(String kmer, String pattern) {//Restituisce true se kmer e' "compatibile" con pattern.

		if(kmer.length()!=pattern.length()) 
			return false;

		String patternKmer = Util.extractPattern(kmer);

		if(!patternKmer.equals(pattern))
			return false;

		return true;
	}

	public static void printKmers(Map<String, Integer> allKmers) {
		System.out.println(allKmers);

	}

	//Estrae i contatori (non le frequenze relative)
	public static Map<String, Integer> kmersCountsExtract(StringBuilder genome, List<String> patterns){

		HashMap<String, Integer> map = new HashMap<String, Integer>();
		int count, k;
		//int den = genome.length()-k+1;

		if(Constant.DEBUG_MODE)
			System.out.println("Length: "+ genome.length());


		for(String pattern : patterns){

			k = pattern.length();

			/* cycle over the length of String till k-mers of length, k, can still be made */
			for(int i = 0; i< (genome.length()-k+1); i++){
				/* output each k-mer of length k, from i to i+k in String*/

				String kmer = genome.substring(i, i+k);
				//System.out.println(kmer);

				if(pattern.contains("0")){
					String spacedWord = Util.extractSpacedWord(kmer, pattern); //kmer deve essere una spaced-word (inexact match).
					kmer = spacedWord;
				}

				if(map.get(kmer)==null)
					count=1;
				else
					count=map.get(kmer) + 1;

				map.put(kmer, count);
			}

		}
		//		if(Constant.DEBUG_MODE)
		//			System.out.println("Counts: "+map);

		return map;
	}

}
