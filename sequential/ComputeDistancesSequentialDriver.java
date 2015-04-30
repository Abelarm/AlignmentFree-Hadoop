package sequential;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import distance.ContextObject;
import distance.DistanceMeasure;
import distance.Parameters;
import distance.d2.EstimatedProb;
import distance.d2.FixedProb;
import distance.d2.UniformProb;
import utility.Constant;
import utility.Util;

/**
 * Compute k-tuple distances between DNA sequences.
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * @author Francesco Gaetano - email: f.gaetano90@gmail.com
 * @author Luigi Lomasto - email: luigilomasto@gmail.com 
 * 
 * @version 4.0
 * 
 * Date: February, 28 2015
 */
public class ComputeDistancesSequentialDriver {

	private String outputDir;
	private String inputFiles;
	private String patternFile;
	private Class<DistanceMeasure>[] distClasses;
	private boolean hasCO;
	private boolean hasCount;
	private int splitStrategy;
	private long startTime;
	private long endTime;

	
	private Map<String, Integer> 	allKmersSeq = null;

	private Map<String, String> allKmersSeqCP = null;

	private List<SequenceCounts> infoSeqs = null;
	private List<SequenceCountsCP> infoSeqsCP = null;


	public ComputeDistancesSequentialDriver(String inputFiles, String patternFile, String outputDir, Class<DistanceMeasure>[] distClasses, boolean hasCO, boolean hasCount, int splitStrategy){
		this.inputFiles = inputFiles;
		this.patternFile = patternFile;
		this.outputDir = outputDir;
		this.distClasses = distClasses;
		this.hasCO = hasCO;
		this.hasCount = hasCount;
		this.splitStrategy = splitStrategy;
		this.startTime=0;
		this.endTime=0;
		
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
		if(splitStrategy==1)
			readFastaAndComputeAllGenome(distances);
		else if(splitStrategy==2)
			readBigFastaAndCompute(distances);

	}

	private void readFastaAndComputeAllGenome(DistanceMeasure[] distances){
		
		startTime=System.currentTimeMillis();
		
		
		startTime=System.currentTimeMillis();

		
		allKmersSeqCP = new HashMap<String,String>();
		allKmersSeq = new HashMap<String,Integer>();

		infoSeqs = new ArrayList<SequenceCounts>();
		infoSeqsCP = new ArrayList<SequenceCountsCP>();

		int l=distances.length;
		String name = "";

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


					line = in.nextLine().trim().toLowerCase();

					if(line.startsWith(">")){


						String oldName = name;
						name = line.substring(1);

						if(seq.length()>0){//processo il contenuto di seq se non e' vuoto
							//if(!seq.equals("")){//processo il contenuto di seq se non e' vuoto
							psIds.println(oldName + "\t"+ seq.length());
							/*Se è presente Co-Phylog devo salvarmi le mappe di tipo Context-Object*/
							if(hasCO){
								allKmersSeqCP = kmersContextObjectCP(seq, patterns); //estrae i kmers per tutti i patterns	
								infoSeqsCP.add(new SequenceCountsCP(allKmersSeqCP, seq.length(), oldName));
								if(Constant.DEBUG_MODE){
									System.out.println("Counts: "+allKmersSeq+"\n");
								}
							}
							if(hasCount){
								for(int i=0;i<l;i++) 
									if(distances[i] instanceof EstimatedProb){
										if(!patterns.contains("1"))
											patterns.add(0, "1");
									}
								allKmersSeq = kmersCountsExtract(seq, patterns); //estrae i kmers per tutti i patterns
								infoSeqs.add(new SequenceCounts(allKmersSeq, seq.length(), oldName)); 
								if(Constant.DEBUG_MODE){
									System.out.println("Counts: "+allKmersSeq+"\n");
								}
							}
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
				if(hasCO){
					allKmersSeqCP = kmersContextObjectCP(seq, patterns); //estrae i kmers per tutti i patterns
					infoSeqsCP.add(new SequenceCountsCP(allKmersSeqCP, seq.length(), name));
				}
				if(hasCount){
					allKmersSeq = kmersCountsExtract(seq, patterns); //estrae i kmers per tutti i patterns
					infoSeqs.add(new SequenceCounts(allKmersSeq, seq.length(), name)); 
					if(Constant.DEBUG_MODE)
						System.out.println("Counts: "+allKmersSeq+"\n");
				}


			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			if(in!=null)
				in.close();

		}

		if(psIds!=null)
			psIds.close();

		/*Aggiunto parametro infoSeqsCP al metodo computeDistances per l'algoritmo Co-Phylog*/
		endTime = System.currentTimeMillis();
		System.out.println("Tempo di esecuzione KmersExtraction = " + (endTime - startTime)+"ms"); 
		
		startTime=System.currentTimeMillis();
		computeDistances(distances, patterns, infoSeqs, infoSeqsCP);
		endTime=System.currentTimeMillis();
		System.out.println("Tempo di esecuzione PairwiseSimilarity = " + (endTime - startTime)+"ms"); 

	}


	private void readBigFastaAndCompute(DistanceMeasure[] distances){

		startTime=System.currentTimeMillis();

		allKmersSeq = new HashMap<String,Integer>();
		allKmersSeqCP = new HashMap<String,String>();

		infoSeqs = new ArrayList<SequenceCounts>();
		infoSeqsCP = new ArrayList<SequenceCountsCP>();

		String name = "";

		int l=distances.length;

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


		File inputF = new File(inputFiles);
		File[] files;

		for(int i=0;i<l;i++) 
			if(distances[i] instanceof EstimatedProb){
				if(!patterns.contains("1"))
					patterns.add(0, "1");
			}

		if(inputF.isDirectory())
			files = inputF.listFiles();
		else
			files = new File[]{inputF};

		for(File f: files){ //Elaboro ogni file di input

			//System.out.println("\nComputo il file: "+f.getName());

			Scanner in = null;
			try {
				in = new Scanner(f);
				String val="";
				String line="";

				int seqLength=0;


				allKmersSeqCP.clear();
				allKmersSeq.clear();

				

				while(in.hasNextLine()){
					
					val = line;
					line = in.nextLine().trim().toLowerCase();

					if(line.startsWith(">")){

						if(val.length()>0){
							seqLength+=val.length();

							if(hasCO){
								kmersContextObjectCP(allKmersSeqCP, val, null, patterns); //estrae i kmers per tutti i patterns
								Map<String, String> toAdd = new HashMap<String,String>();
								toAdd.putAll(allKmersSeqCP);
								infoSeqsCP.add(new SequenceCountsCP(toAdd,seqLength,name));

							}
							if(hasCount){
								kmersCountsExtract(allKmersSeq, val, null, patterns); //estrae i kmers per tutti i patterns
								Map<String, Integer> toAdd = new HashMap<String,Integer>();
								toAdd.putAll(allKmersSeq);
								infoSeqs.add(new SequenceCounts(toAdd, seqLength, name)); 

								if(Constant.DEBUG_MODE)
									System.out.println("Counts: "+allKmersSeq+"\n");
							}
							allKmersSeqCP.clear();
							allKmersSeq.clear();
							seqLength=0;

						}
						name = line.substring(1);

						val = line;
						if(in.hasNext())
							line = in.nextLine().trim().toLowerCase();



						if(val.startsWith(">")){
							val = line;
							if(in.hasNext())
								line = in.nextLine().trim().toLowerCase();
						}
					}
					seqLength+=val.length();


					if(hasCO){
						//System.out.println("genome: "+val +"\t"+"succ: "+line);
						kmersContextObjectCP(allKmersSeqCP, val, line, patterns); //estrae i kmers per tutti i patterns	
					}
					if(hasCount){
						kmersCountsExtract(allKmersSeq, val, line, patterns); //estrae i kmers per tutti i patterns
						if(Constant.DEBUG_MODE)
							System.out.println("Counts: "+allKmersSeq+"\n");
					}

					

				}

				val=line;
				seqLength+=val.length();

				if(hasCO){
					kmersContextObjectCP(allKmersSeqCP, val,null, patterns); //estrae i kmers per tutti i patterns
					Map<String, String> toAdd = new HashMap<String,String>();
					toAdd.putAll(allKmersSeqCP);
					infoSeqsCP.add(new SequenceCountsCP(toAdd, seqLength, name));

				}
				if(hasCount){
					kmersCountsExtract(allKmersSeq, val, null, patterns); //estrae i kmers per tutti i patterns
					Map<String, Integer> toAdd = new HashMap<String,Integer>();
					toAdd.putAll(allKmersSeq);
     				infoSeqs.add(new SequenceCounts(toAdd, seqLength, name)); 

					if(Constant.DEBUG_MODE)
						System.out.println("Counts: "+allKmersSeq+"\n");
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			if(in!=null)
				in.close();

		}

		if(psIds!=null)
			psIds.close();

		/*Aggiunto parametro infoSeqsCP al metodo computeDistances per l'algoritmo Co-Phylog*/
		
		endTime = System.currentTimeMillis();
		System.out.println("Tempo di esecuzione KmersExtraction = " + (endTime - startTime)+"ms"); 
		startTime =  System.currentTimeMillis();
		computeDistances(distances, patterns, infoSeqs, infoSeqsCP);
		endTime= System.currentTimeMillis();;
		System.out.println("Tempo di esecuzione PairwiseSimilarity = " + (endTime - startTime)+"ms"); 



	
	}

	
	private void kmersContextObjectCP(Map<String, String> allKmersSeqCP, String genome, String succ, List<String> patterns){
		String context="";
		String object ="";
		int  k;
		String original=genome;

		String [] CO;
		//StringBuilder concat = genome.append(Util.reverseComplement(genome));
		if(Constant.DEBUG_MODE)
			System.out.println("Length: "+ genome.length());


		for(String pattern : patterns){
			//System.out.println("Computo il pattern: "+pattern);
			if(Util.isValidSpacedWordPattern(pattern)){
				if(Util.isCompatibile(pattern)){

					k = pattern.length();


					/*assumiamo che ogni riga ha lunghezza maggiore di k*/
					if(succ!=null){
						if(succ.length()<(k-1))
							continue;
						String temp = succ.substring(0, (k-1));
						genome=original;
						genome=genome+temp;
					}
					/* cycle over the length of String till k-mers of length, k, can still be made */
					for(int i = 0; i< (genome.length()-k+1); i++){
						/* output each k-mer of length k, from i to i+k in String*/

						String kmer = genome.substring(i, i+k);
						
						CO=Util.extractCurrentCO(kmer, pattern);
						context=CO[0];
						object=CO[1];

						String val = allKmersSeqCP.get(context); 
						//System.out.println("Kmer corrente: "+kmer+ "\tOggetto corrente: "+object+"\tContesto: "+context );

						if(val!=null){
							if(!val.equals(Constant.MARKED)){
								if(!object.equalsIgnoreCase(val)){
									allKmersSeqCP.put(context, Constant.MARKED);
								}
							}
						}
						else
							allKmersSeqCP.put(context, object);

		


						object="";
						context="";


					}
				}
			}
		}
	}



	
	private static void kmersCountsExtract(Map<String, Integer> allKmersSeq, String genome, String succ, List<String> patterns){

		int count, k;
		//int den = genome.length()-k+1;
		String original=genome;

		if(Constant.DEBUG_MODE)
			System.out.println("Length: "+ genome.length());


		for(String pattern : patterns){


			if(Util.isValidSpacedWordPattern(pattern)){

				k = pattern.length();


				if(succ!=null){

					if(succ.length()<(k-1))
						continue;
					String temp = succ.substring(0, (k-1));
					genome=original;
					genome=genome+temp;
				}

				/* cycle over the length of String till k-mers of length, k, can still be made */
				for(int i = 0; i< (genome.length()-k+1); i++){
					/* output each k-mer of length k, from i to i+k in String*/

					String kmer = genome.substring(i, i+k);
					
					
					
					if(pattern.contains("0")){
						String spacedWord;
						if(pattern.charAt(0)=='1' && pattern.charAt(pattern.length()-1)=='1'){
							spacedWord = Util.extractSpacedWord(kmer, pattern); //kmer deve essere una spaced-word (inexact match).
							kmer = spacedWord;
						}
					}

					if(allKmersSeq.get(kmer)==null)
						count=1;
					else
						count=allKmersSeq.get(kmer) + 1;
					
					allKmersSeq.put(kmer, count);
				}
			}
		}
		//		if(Constant.DEBUG_MODE)
		//			System.out.println("Counts: "+map);


	}



	private void computeDistances(DistanceMeasure[] distances, List<String> patterns, List<SequenceCounts> infoSeqs, List<SequenceCountsCP> infoSeqsCP) {

		//Misure di distanza
		NumberFormat formatter = new DecimalFormat("#0.0000000");
		double[][] matrix;
		double[][] sumMatrix;
		int patNumber;
		int start;
		double val;
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
			patNumber=0;
			if(d instanceof ContextObject)
				sumMatrix=new double [infoSeqsCP.size()][infoSeqsCP.size()];
			else
				sumMatrix=new double [infoSeqs.size()][infoSeqs.size()];
			for(String pattern : patterns){
				if(Util.isValidSpacedWordPattern(pattern)){
					int k = pattern.length();
					patNumber++;
					if(Constant.PRINT_MATRIX_SEQ)
						matrix = new double[infoSeqs.size()][infoSeqs.size()];

					if(Constant.SEQUENTIAL_PRINT_DISTANCES){
						if(pattern.contains("0"))
							System.out.println("\n"+k+"-mers extractions with distance "+d.getName()+" and pattern "+pattern);
						else
							System.out.println("\n"+k+"-mers extractions with distance "+d.getName());
					}



					if(d instanceof ContextObject ){

						if(d.isCompatibile(pattern)){

							for(int i=0; i<infoSeqsCP.size(); i++){
								if(d.isSymmetricMeasure())
									start = i+1;
								else
									start = 0;

								for(int j=start; j<infoSeqsCP.size(); j++){


									val = computeDissimilarityMeasure(d, infoSeqsCP.get(i), infoSeqsCP.get(j), pattern);


									sumMatrix[i][j]+=val;

									if(Constant.PRINT_MATRIX_SEQ)
										if(d.isSymmetricMeasure())
											matrix[j][i] = matrix[i][j] = val;
										else
											matrix[i][j] = val;

									if(Constant.SEQUENTIAL_PRINT_DISTANCES){
										System.out.println("Pattern "+pattern+" d_"+d.getName()+"("+infoSeqsCP.get(i).getNameCP()+","+infoSeqsCP.get(j).getNameCP()+")="+formatter.format(val).replace(",", "."));
										//System.out.println("d("+infoSeqs.get(i).getName()+","+infoSeqs.get(j).getName()+")="+val);
									}

									if(psDistances!=null)
										psDistances.println("Pattern "+pattern+" d_"+d.getName()+"("+infoSeqsCP.get(i).getNameCP()+","+infoSeqsCP.get(j).getNameCP()+")="+val);
								}
							}
						}
					}
					else{
						if(d.isCompatibile(pattern)){

							for(int i=0; i<infoSeqs.size(); i++){
								if(d.isSymmetricMeasure())
									start = i+1;
								else
									start = 0;

								for(int j=start; j<infoSeqs.size(); j++){

									val = computeDissimilarityMeasure(d, infoSeqs.get(i), infoSeqs.get(j), pattern);

									sumMatrix[i][j]+=val;

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
			/*
			if(Constant.PRINT_MATRIX_SEQ){
				System.out.println("Stampo la matrice delle somme delle distanze");
				printDistanceMatrix(sumMatrix,infoSeqs);


			}
			if(Constant.PRINT_AVG_DISTANCES)
			   AVGPatterns(sumMatrix,patNumber,infoSeqs.size());

			}*/
			//	AVGPatterns(sumMatrix,patNumber,infoSeqs.size());

		}

		if(psDistances!=null)
			psDistances.close();

	}
	/*Metodo che lalcola la media tra distanze di pattern */
	private void AVGPatterns(double matrix[][],int patNumber,int size){
		NumberFormat formatter = new DecimalFormat("#0.0000000");
		double avg;
		for(int i=0;i<size;i++){
			for(int j=0;j<size;j++){
				avg=matrix[i][j]/patNumber;
				System.out.println("Distanza media tra "+(i+1)+" "+(j+1)+": "+formatter.format(avg).replace(",", ".")+"\t");
			}

		}

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
			}
			System.out.println();
		}
	} 


	//Frequenze non normalizzate
	private static double computeDissimilarityMeasure(DistanceMeasure distance, SequenceCounts seq1, SequenceCounts seq2, String pattern) {//Usata per misure simmetriche

			
		double res, dist = distance.initDistance();
		Integer c1, c2;
		int k = pattern.length();
		int numEl=0;
		Parameters param = new Parameters();

		Set<String> set1 = seq1.getAllCountKmers().keySet();
		Set<String> set2 = seq2.getAllCountKmers().keySet();

		Set<String> mySet = new HashSet<String>(); // The variable "mySet" does not support the add or addAll operations.
		mySet.addAll(set1);
		mySet.addAll(set2);

		Iterator<String> it = mySet.iterator();
		while(it.hasNext()){

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

			param.setC1(c1);
			param.setC2(c2);
			param.setLength1(seq1.getLength());
			param.setLength2(seq2.getLength());
			param.setK(k);
			param.setKmer(kmerCurr);


			if(distance instanceof EstimatedProb){
				param.setMapS1(seq1.getAllCountKmers());
				param.setMapS2(seq2.getAllCountKmers());
				//	System.out.println("SEQUENTIAL-m1: "+seq1.getAllCountKmers()+"\tm2"+seq2.getAllCountKmers());

			}
			if(distance instanceof UniformProb){
				if(Constant.IS_PROTEIN)
					param.setNumLettere(Constant.PROTEIN_ALPHABET.length());
				else
					param.setNumLettere(Constant.DNA_ALPHABET.length());
			}
			if(distance instanceof FixedProb){

				HashMap<String, Map<String, Double>> probabilities = readProbabilities(System.getProperty("user.home")+Constant.LOCAL_PATH_PREFIX+Constant.PROBABILITIES_PATH);	
				if(probabilities.containsKey("ALL")){
					param.setProbMap1(probabilities.get("ALL"));
					param.setProbMap2(probabilities.get("ALL"));
				}else{
					if(probabilities.containsKey("all")){
						param.setProbMap1(probabilities.get("all"));
						param.setProbMap2(probabilities.get("all"));
					}else{
						param.setProbMap1(probabilities.get(seq1.getName()));
						param.setProbMap2(probabilities.get(seq2.getName()));
					}
				}
			}


			res = distance.computePartialDistance(param); 

			//res = distance.computePartialDistance(c1, seq1.getLength(), c2, seq2.getLength(), k);
			dist = distance.distanceOperator(dist, res);
			numEl++;
		}

		dist = distance.finalizeDistance(dist,numEl);

		return dist;
	}

	/*Metodo che calcola le distanze per l'algoritmo Co-Phylog*/
	private static double computeDissimilarityMeasure(DistanceMeasure distance, SequenceCountsCP seq1, SequenceCountsCP seq2, String pattern) {//Usata per misure simmetriche
		double res, dist = distance.initDistance();
		int validContext=0;
		int k = pattern.length();

		//System.out.println(seq1+"\t"+seq2);
		Set<String> set1 = seq1.getAllCountKmersCP().keySet();
		Set<String> set2 = seq2.getAllCountKmersCP().keySet();

		Set<String> mySet=new HashSet<String>();

		mySet.addAll(set1);
		mySet.addAll(set2);

		Parameters param = new Parameters();

		//System.out.println("---"+mySet);

		Iterator<String> it = mySet.iterator();

		while(it.hasNext()){

			String current = it.next();

			//System.out.println(kmerCurr+"\t"+pattern);

			if(!match(current, pattern))
				continue;



			String object1 = seq1.getAllCountKmersCP().get(current);
			String object2 = seq2.getAllCountKmersCP().get(current);

			//il problema stava qui, perchè se un contesto non c'è in una delle due va in nullpointerexception, ricerca una chiave che non c'è, quindi controllo prima se c'è
			if((seq1.getAllCountKmersCP().containsKey(current) && seq2.getAllCountKmersCP().containsKey(current)) && (object1!=Constant.MARKED && object2!=Constant.MARKED )){
				validContext++;
				//System.out.println("Contesto: "+current+"\tOggetto: "+seq1.getAllCountKmersCP().get(current)+" "+validContext);
				param.setContesto1(current);
				param.setContesto2(current);
				param.setOggetto1(object1);
				param.setOggetto2(object2);
				param.setK(k);
				res = distance.computePartialDistance(param); 
				dist = distance.distanceOperator(dist, res);

			}	


		}

		//per calcolare la distanza finale bisogna fare dist/R, dove R è la lunghezza dei contesti validi
		//	System.out.println("Num contesti: "+validContext+"\tNum oggetti: "+dist);

		dist = distance.finalizeDistance(dist,validContext);
		//System.out.println("Distanza: "+dist);
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

		Map<String, Integer> map = new HashMap<String, Integer>();
		kmersCountsExtract(map, genome.toString(),null, patterns);
		return map;
		/*	
		
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		int count, k;
		//int den = genome.length()-k+1;

		if(Constant.DEBUG_MODE)
			System.out.println("Length: "+ genome.length());


		for(String pattern : patterns){

			if(Util.isValidSpacedWordPattern(pattern)){

				k = pattern.length();

				for(int i = 0; i< (genome.length()-k+1); i++){

					String kmer = genome.substring(i, i+k);
					//System.out.println(kmer);

					if(pattern.contains("0")){
						String spacedWord;
						if(pattern.charAt(0)=='1' && pattern.charAt(pattern.length()-1)=='1'){
							spacedWord = Util.extractSpacedWord(kmer, pattern); //kmer deve essere una spaced-word (inexact match).
							kmer = spacedWord;
						}
					}

					if(map.get(kmer)==null)
						count=1;
					else
						count=map.get(kmer) + 1;

					map.put(kmer, count);
				}
			}
		}
		//		if(Constant.DEBUG_MODE)
		//			System.out.println("Counts: "+map);

		return map; 
		*/ 
	}



	/*Metodo di estrazione Context-Object per Co-Phylog*/
	private Map<String, String> kmersContextObjectCP(StringBuilder genome, List<String> patterns) {

		HashMap<String,String> map = new HashMap<String,String>();
		kmersContextObjectCP(map,genome.toString(),null,patterns);
		return map;
    /*		
		HashMap<String,String> map = new HashMap<String,String>();

		
		String CO[];
		String context="";
		String object ="";

		int  k;
		//StringBuilder concat = genome.append(Util.reverseComplement(genome));

		if(Constant.DEBUG_MODE)
			System.out.println("Length: "+ genome.length());


		for(String pattern : patterns){

			//map.clear();

			if(Util.isValidSpacedWordPattern(pattern)){
				if(Util.isCompatibile(pattern)){



					k = pattern.length();

					for(int i = 0; i< (genome.length()-k+1); i++){

						String kmer = genome.substring(i, i+k);
						String val=null;

						CO=Util.extractCurrentCO(kmer, pattern);
						context=CO[0];
						object=CO[1];

						val=map.get(context);
						//System.out.println("Kmer corrente: "+kmer+ "\tOggetto corrente: "+object+"\tContesto: "+context );

						if(val!=null){
							if(!val.equals(Constant.MARKED)){
								if(!object.equalsIgnoreCase(val)){
									map.put(context, Constant.MARKED);
								}
							}
						}
						else{
							map.put(context, object);
						}


						object="";
						context="";


					}
				}
			}
		}



		
		Set<String> chiavi= map.keySet();
		Iterator<String> iter= chiavi.iterator();
		while(iter.hasNext()){
			String cont=iter.next();
			System.out.println("context: "+cont+" oggetto: "+map.get(cont));

		}
	

		//System.out.println();
		return map;   
		*/
	}

	private HashMap<String, String> KmerCOReverseExtract(HashMap<String, String> map, StringBuilder genome, List<String> patterns) {
		
		/*Qui mi devi richiamare la funzione per il reverse della stringa*/
		StringBuilder reverse = Util.reverseComplement(genome);

		String context="";
		String object = null;
		int  k;

		if(Constant.DEBUG_MODE)
			System.out.println("Length: "+ genome.length());


		for(String pattern : patterns){

			k = pattern.length();

			/* cycle over the length of String till k-mers of length, k, can still be made */
			for(int i = 0; i< (reverse.length()-k+1); i++){
				/* output each k-mer of length k, from i to i+k in String*/

				String kmer = reverse.substring(i, i+k);


				if(pattern.contains("0")){
					for(int j=0; j<kmer.length(); j++){
						if(pattern.charAt(j)=='0')
							object=kmer.charAt(j)+"";
						else
							context+=kmer.charAt(j);
					}	
					String val = map.get(context); 
					//System.out.println("Kmer corrente: "+kmer+ "\tOggetto corrente: "+object+"\tContesto: "+context );

					if(val!=null){
						if(!val.equals(Constant.MARKED)){
							if(!object.equalsIgnoreCase(val)){
								map.put(context, Constant.MARKED);
							}
						}
					}
					else{
						map.put(context, object);
					}
					context="";
				}
			}
		}
		return map;
	}

	private static HashMap<String, Map<String, Double>> readProbabilities(String probFile) { 
		System.out.println(probFile);
		HashMap<String,Map<String, Double>> listaProb = new HashMap<String, Map<String, Double>>();
		HashMap<String, Double> prob=null;

		try {
			Scanner in = new Scanner(new File(probFile));
			String currLine;
			String currID="";
			while(in.hasNextLine()){
				currLine = in.nextLine();
				if(currLine.contains(">")){
					if(prob!=null)
						listaProb.put(currID, prob); //metto la mappa precedente
					currID=currLine.substring(1).toLowerCase();
					prob = new HashMap<String, Double>();
				}
				else{
					String letter=currLine.substring(0, 1);
					double probability = Double.parseDouble(currLine.substring(2));
					prob.put(letter, probability);
				}
			}
			listaProb.put(currID, prob); //inserisco l'ultima mappa
			in.close();

		} catch (Exception e) {
			//e.printStackTrace();
			//genero probabilità uniformi?
		}

		return listaProb;
	}

}