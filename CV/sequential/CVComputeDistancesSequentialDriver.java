package CV.sequential;

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
import java.util.Map.Entry;

import CV.distance.DistanceMeasure;
import CV.distance.DistanceMeasureDouble;
import CV.distance.Parameters;
import utility.Constant;
import utility.Util;

/**
 * Compute k-tuple distances between DNA sequences, using the Composition Vector method.
 * 
 * @author Luigi Giugliano - Steven Rosario Sirchia
 * 
 * @version 4.0
 * 
 * Date: February, 16 2015
 */
public class CVComputeDistancesSequentialDriver {

	private String outputDir;
	private String inputFiles;
	private String patternFile;
	private Class<DistanceMeasure>[] distClasses;
	private boolean hasCount;
	private int splitStrategy;
	private long startTime;
	private long endTime;
	private ArrayList<String> appoggio = null;
	private Map<String, Integer> 	allKmersSeq = null;

	private List<Sequence4Counts> infoSeqs = null;
	private List<SequenceA> infoSeqsWithA = null;





	public CVComputeDistancesSequentialDriver(String inputFiles, String patternFile, String outputDir, Class<DistanceMeasure>[] distClasses, boolean hasCO, boolean hasCount, int splitStrategy){
		this.inputFiles = inputFiles;
		this.patternFile = patternFile;
		this.outputDir = outputDir;
		this.distClasses = distClasses;
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

	/**
	 * Method for computing genomes following the splitStrategy 1
	 * @param distances array of the distance classes
	 * */
	private void readFastaAndComputeAllGenome(DistanceMeasure[] distances){

		startTime=System.currentTimeMillis();
		
		allKmersSeq = new HashMap<String,Integer>();

		infoSeqs = new ArrayList<Sequence4Counts>();
		infoSeqsWithA = new ArrayList<SequenceA>();

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
							
								for(int i=0;i<l;i++) 
									if(distances[i].getName().contains("EstimatedProbD2")){
										if(!patterns.contains("1"))
											patterns.add(0, "1");
									}
								allKmersSeq = kmersCountsExtract(seq, patterns); //estrae i kmers per tutti i patterns
								HashMap<String, ArrayList<Double>> all4Counts = this.generate4Counts(allKmersSeq, patterns);
								infoSeqs.add(new Sequence4Counts(all4Counts, seq.length(), oldName));
								
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
					HashMap<String, ArrayList<Double>> all4Counts = this.generate4Counts(allKmersSeq, patterns);
					infoSeqs.add(new Sequence4Counts(all4Counts, seq.length(), name));
					if(Constant.DEBUG_MODE)
						System.out.println("Counts: "+allKmersSeq+"\n");
				
				infoSeqsWithA = this.generateA(infoSeqs);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			if(in!=null)
				in.close();

		}

		if(psIds!=null)
			psIds.close();

		endTime = System.currentTimeMillis();
		System.out.println("Tempo di esecuzione KmersExtraction = " + (endTime - startTime)+"ms"); 
		
		startTime=System.currentTimeMillis();
		computeDistances(distances, patterns, infoSeqsWithA);
		endTime=System.currentTimeMillis();
		System.out.println("Tempo di esecuzione PairwiseSimilarity = " + (endTime - startTime)+"ms"); 
	}
	
	
	/**
	 * Method used for computing genomes following the splitStrategy 2
	 * */
	private void readBigFastaAndCompute(DistanceMeasure[] distances){

		startTime=System.currentTimeMillis();

		allKmersSeq = new HashMap<String,Integer>();

		infoSeqs = new ArrayList<Sequence4Counts>();

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
			if(distances[i].getName().contains("EstimatedProbD2")){
				if(!patterns.contains("1"))
					patterns.add(0, "1");
			}

		if(inputF.isDirectory())
			files = inputF.listFiles();
		else
			files = new File[]{inputF};

		for(File f: files){ //Elaboro ogni file di input
			//if(Constant.DEBUG_MODE)
				System.out.println("\nComputo il file: "+f.getName());
			//int numlinea=0;
			Scanner in = null;
			try {
				in = new Scanner(f);
				String val="";
				String line="";

				int seqLength=0;


				allKmersSeq.clear();

				

				while(in.hasNextLine()){
					val = line;
					line = in.nextLine().trim().toLowerCase();
					
//					numlinea++;
//					System.out.println("In processing linea "+numlinea);
					
					if(line.startsWith(">")){

						if(val.length()>0){
							seqLength+=val.length();

							
							
								if(Constant.DEBUG_MODE)
									System.out.println("Corrente: "+val +"\t"+"Successiva: "+line +"idSeq"+"\t Length"+seqLength);
								
								kmersCountsExtract(allKmersSeq, val, null, patterns); //estrae i kmers per tutti i patterns
								//System.out.println("Counts: "+allKmersSeq+"\n");
								Map<String, Integer> toAdd = new HashMap<String,Integer>();
								toAdd.putAll(allKmersSeq);
								//System.out.println(toAdd);
								HashMap<String, ArrayList<Double>> all4Counts = this.generate4Counts(allKmersSeq, patterns);
								//infoSeqs.add(new SequenceCountsCV(toAdd, seqLength, name)); 

								if(Constant.DEBUG_MODE)
									System.out.println("Counts: "+allKmersSeq+"\n");
							
							allKmersSeq.clear();
							seqLength=0;

						}
						String oldName = name;
						name = line.substring(1);

						val = line;
						if(in.hasNext())
							line = in.nextLine().trim().toLowerCase();



						if(val.startsWith(">")){
							val = line;
							if(in.hasNext())
								line = in.nextLine().trim().toLowerCase();
							//else
								//line="";
						}
					}
					seqLength+=val.length();


					
					
						if(Constant.DEBUG_MODE)
							System.out.println("Corrente: "+val +"\t"+"Successiva: "+line +"idSeq"+"\t Length"+seqLength);


						kmersCountsExtract(allKmersSeq, val, line, patterns); //estrae i kmers per tutti i patterns
						//System.out.println("Counts: "+allKmersSeq+"\n");
						if(Constant.DEBUG_MODE)
							System.out.println("Counts: "+allKmersSeq+"\n");
					

					val = line;
					if(in.hasNext())
						line = in.nextLine().trim().toLowerCase();

				}

				val=line;

				seqLength+=val.length();


				
				
					if(Constant.DEBUG_MODE)
						System.out.println("Corrente: "+val +"\t"+"Successiva: "+"Fine File"+"\t Lenght"+seqLength);

					kmersCountsExtract(allKmersSeq, val, null, patterns); //estrae i kmers per tutti i patterns
					//System.out.println("Counts: "+allKmersSeq+"\n");
					Map<String, Integer> toAdd = new HashMap<String,Integer>();
					toAdd.putAll(allKmersSeq);
					HashMap<String, ArrayList<Double>> all4Counts = this.generate4Counts(allKmersSeq, patterns);
					infoSeqs.add(new Sequence4Counts(all4Counts, seqLength, name)); 

					//System.out.println(toAdd);
					if(Constant.DEBUG_MODE)
						System.out.println("Counts: "+allKmersSeq+"\n");
				
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			if(in!=null)
				in.close();

		}

		if(psIds!=null)
			psIds.close();

		infoSeqsWithA = this.generateA(infoSeqs);
		
		endTime = System.currentTimeMillis();
		System.out.println("Tempo di esecuzione KmersExtraction = " + (endTime - startTime)+"ms"); 
		startTime =  System.currentTimeMillis();
		computeDistances(distances, patterns, infoSeqsWithA);
		endTime= System.currentTimeMillis();;
		System.out.println("Tempo di esecuzione PairwiseSimilarity = " + (endTime - startTime)+"ms"); 
	}

	/**
	 * Method for extracting the count of kmers in a string
	 * @param allKmersSeq Map kmer->count
	 * @param genome the input string
	 * @param succ the next input string
	 * @param patterns list of the patterns to apply
	 * */
	private void kmersCountsExtract(Map<String, Integer> allKmersSeq, String genome, String succ, List<String> patterns){
		
		String original=genome;
		int count, k;
		//int den = genome.length()-k+1;


		if(Constant.DEBUG_MODE)
			System.out.println("Length: "+ genome.length());

		//calcolo e aggiungo i pattern di lunghezza k-1 e k-2 necessari per CV
		appoggio = new ArrayList<String>();
		for(String pattern : patterns){
			if(pattern.contains("0")||pattern.length()<3)
				appoggio.add(pattern);
			else{
				String s1, s2;
				s1=pattern.substring(0, pattern.length()-1);
				s2=pattern.substring(1, pattern.length()-1);
				if(!patterns.contains(s1))
					appoggio.add(s1);
				if(!patterns.contains(s2))
					appoggio.add(s2);
			}
		}
		
		Set s = new HashSet<String>(patterns);
		Set a = new HashSet<String>(appoggio);
		
		s.addAll(a);
		patterns = new ArrayList<String>(s);
		
		patterns.sort(String.CASE_INSENSITIVE_ORDER);

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
					//System.out.println(kmer);


					if(allKmersSeq.get(kmer)==null)
						count=1;
					else
						count=allKmersSeq.get(kmer) + 1;

					//	System.out.println(kmer+"\t"+count);
					allKmersSeq.put(kmer, count);
				}
			}
		}
		//		if(Constant.DEBUG_MODE)
		//			System.out.println("Counts: "+map);


	}

	/**
	 * Method that generate a list containing the 4 counts (kmer, kmer-1(start), kmer-1(end), kmer-2)
	 * needed for calculating the composition vector
	 * @param allSeqCounts map kmer->count (including kmer-1(start), kmer-1(end) and kmer-2 counts as separate entries)
	 * @param patterns list of patterns
	 * @return Map kmer->4 counts
	 * */
	private HashMap<String, ArrayList<Double>> generate4Counts(Map<String, Integer> allSeqCounts, List<String> patterns){
		
		HashMap<String, ArrayList<Double>> all4Counts = new HashMap<String, ArrayList<Double>>();
		
		//rimuovo i pattern aggiunti per CV (su cui non calcolare le distanze)
		patterns.removeAll(appoggio);
		int k;
		String kmer;
		
		for(String pattern : patterns){
		
			k = pattern.length();
			Iterator<Entry<String,Integer>> it= allSeqCounts.entrySet().iterator();
			Entry<String,Integer> val;
			String kmer1,kmer1b,kmer2;
			double c0,c1,c1b,c2;
			
			while(it.hasNext()){
				val=it.next();
				kmer=val.getKey();
				if(kmer.length()!=k || kmer.length()<3 || kmer.contains("*"))
					continue;
				
				kmer1=kmer.substring(0, k-1);
				kmer1b=kmer.substring(1, k);
				kmer2=kmer.substring(1, k-1);

				c0=val.getValue();

				
				if(allSeqCounts.containsKey(kmer1)){
					c1=allSeqCounts.get(kmer1);
				}
				else
					c1=0;

				if(allSeqCounts.containsKey(kmer1b)){
					c1b=allSeqCounts.get(kmer1b);
				}
				else
					c1b=0;

				if(allSeqCounts.containsKey(kmer2)){
					c2=allSeqCounts.get(kmer2);
				}
				else
					c2=0;

				ArrayList<Double> allC = new ArrayList<Double>();
				allC.add(c0);
				allC.add(c1);
				allC.add(c1b);
				allC.add(c2);
				all4Counts.put(kmer, allC);
			}
		}
		return all4Counts;
	}

	/**
	 * Method for generating the composition vector of a genome, 
	 * starting from the list of its "4 counts"
	 * @param infoSeqs list of entries Kmer->4 counts
	 * @return list of entries kmer->A
	 * */
	private List<SequenceA> generateA(List<Sequence4Counts> infoSeqs){
		
		ArrayList<SequenceA> infoSeqsWithA = new ArrayList<SequenceA>();
		Iterator<Sequence4Counts> it = infoSeqs.iterator();
		Sequence4Counts currentInfo;
		Map<String, ArrayList<Double>> seq4Counts;
		
		
		while(it.hasNext()){
			currentInfo = it.next();
			seq4Counts = currentInfo.getAllCountKmers();
			int seqLength = currentInfo.getLength();
			Map<String, Double> seqA = new HashMap<String, Double>();
			
			Iterator<Entry<String, ArrayList<Double>>> it2 = seq4Counts.entrySet().iterator();
			Entry<String, ArrayList<Double>> currKmer;
			while(it2.hasNext()){
				currKmer=it2.next();
				double p, p1, p1b, p2, p0, a;
				int k= currKmer.getKey().length();
				ArrayList<Double> counts = currKmer.getValue();
				p=(counts.get(0))/(seqLength-k+1);
				p1=(counts.get(1))/(seqLength-(k-1)+1);
				p1b=(counts.get(2))/(seqLength-(k-1)+1);
				p2=(counts.get(3))/(seqLength-(k-2)+1);
				if(p2==0)
					p0=0;
				else
					p0=(p1*p1b)/p2;
				a=(p-p0)/p0;
				seqA.put(currKmer.getKey(), a);
			}
			infoSeqsWithA.add(new SequenceA(seqA, currentInfo.getLength(), currentInfo.getName()));
		}
		return infoSeqsWithA;
	}
	
	/**
	 * Method for computing distances on Composition Vectors of genomes
	 * @param distances array of distance classes
	 * @param patterns list of patterns
	 * @param infoSeqsWithA list of sequences infos and Composition vectors
	 * */
	private void computeDistances(DistanceMeasure[] distances, List<String> patterns, List<SequenceA> infoSeqsWithA) {

		//System.out.println(infoSeqsCP);
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
			
			sumMatrix=new double [infoSeqsWithA.size()][infoSeqsWithA.size()];
			for(String pattern : patterns){
				if(Util.isValidSpacedWordPattern(pattern)){
					int k = pattern.length();
					patNumber++;
					if(Constant.PRINT_MATRIX_SEQ)
						matrix = new double[infoSeqsWithA.size()][infoSeqsWithA.size()];

					if(pattern.length()==1)
						continue;

					if(Constant.SEQUENTIAL_PRINT_DISTANCES){
						if(pattern.contains("0"))
							System.out.println("\n"+k+"-mers extractions with distance "+d.getName()+" and pattern "+pattern);
						else
							System.out.println("\n"+k+"-mers extractions with distance "+d.getName());
					}

						if(d.isCompatibile(pattern)){

							for(int i=0; i<infoSeqsWithA.size(); i++){
								if(d.isSymmetricMeasure())
									start = i+1;
								else
									start = 0;

								for(int j=start; j<infoSeqsWithA.size(); j++){

									val = computeDissimilarityMeasure(d, infoSeqsWithA.get(i), infoSeqsWithA.get(j), pattern);

									sumMatrix[i][j]+=val;

									if(Constant.PRINT_MATRIX_SEQ)
										if(d.isSymmetricMeasure())
											matrix[j][i] = matrix[i][j] = val;
										else
											matrix[i][j] = val;

									if(Constant.SEQUENTIAL_PRINT_DISTANCES){
										System.out.println("Pattern "+pattern+" d_"+d.getName()+"("+infoSeqsWithA.get(i).getName()+","+infoSeqsWithA.get(j).getName()+")="+formatter.format(val).replace(",", "."));
										//System.out.println("d("+infoSeqs.get(i).getName()+","+infoSeqs.get(j).getName()+")="+val);
									}

									if(psDistances!=null)
										psDistances.println("Pattern "+pattern+" d_"+d.getName()+"("+infoSeqsWithA.get(i).getName()+","+infoSeqsWithA.get(j).getName()+")="+val);
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
	
	/**
	 * Utility method for reading patterns from their file
	 * @param filename file name
	 * @return list of patterns
	 * */
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

	/**
	 * Method for printing the Distance Matrix
	 * */
	public static void printDistanceMatrix(double[][] matrix, List<Sequence4Counts> seqs){
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

	/**
	 * Method for printing the Distance Matrix
	 * */
	public static void printDistanceMatrixStd(double[][] matrix, List<Sequence4Counts> seqs){
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

	
	/**
	 * Method for computing the distance between seq1 and seq2, following the pattern
	 * @param distance distance to apply
	 * @param seq1 info of the first sequence (CV included)
	 * @param seq2 info of the second sequence (CV included)
	 * @param pattern pattern to follow
	 * @return value of distance
	 * */
	private static double computeDissimilarityMeasure(DistanceMeasure distance, SequenceA seq1, SequenceA seq2, String pattern) {//Usata per misure simmetriche



		double dist = distance.initDistance();
		Double a1, a2;
		int k = pattern.length();
		Parameters param = new Parameters();

		Set<String> set1 = seq1.getAllA().keySet();
		Set<String> set2 = seq2.getAllA().keySet();

		Set<String> mySet = new HashSet<String>(); // The variable "mySet" does not support the add or addAll operations.
		mySet.addAll(set1);
		mySet.addAll(set2);

		ArrayList<Double> partial = new ArrayList<Double>();
		ArrayList<Double> curr = new ArrayList<Double>();
		curr.add(0, 0.0);
		curr.add(1, 0.0);
		curr.add(2, 0.0);
		double c, p;
		c=p=0.0;
		
		Iterator<String> it = mySet.iterator();
		while(it.hasNext()){

			String kmerCurr = it.next();

			//Se il kmer in esame e' compatibile con il pattern allora procedo.
			if(!match(kmerCurr, pattern))
				continue;

			a1 = seq1.getAllA().get(kmerCurr);
			a2 = seq2.getAllA().get(kmerCurr);

			if(a1==null)
				a1=0.0;

			if(a2==null)
				a2=0.0;
			
			int l1 = seq1.getLength();
			int l2 = seq2.getLength();
			
			param.setC1(a1);
			param.setC2(a2);
			param.setLength1(l1);
			param.setLength2(l2);
			param.setK(k);
			param.setKmer(kmerCurr);
			
			if(distance instanceof DistanceMeasureDouble){
				partial = ((DistanceMeasureDouble)distance).computePartialDistanceDouble(param);
				curr = ((DistanceMeasureDouble)distance).distanceOperatorDouble(partial, curr);
			}else{
				p = distance.computePartialDistance(param);
				c = distance.distanceOperator(p, c);
			}
		}
		
		if(distance instanceof DistanceMeasureDouble){
			dist = ((DistanceMeasureDouble)distance).finalizeDistanceDouble(curr, 0);
		}else{
			dist = distance.finalizeDistance(c, 0);
		}
		return dist;
	}

	/**
	 * Method for checking if a kmer matches a pattern
	 * */
	private static boolean match(String kmer, String pattern) {//Restituisce true se kmer e' "compatibile" con pattern.

		if(kmer.length()!=pattern.length()) 
			return false;

		String patternKmer = Util.extractPattern(kmer);

		if(!patternKmer.equals(pattern))
			return false;

		return true;
	}

	/**
	 * prints all kmers
	 */
	public static void printKmers(Map<String, Integer> allKmers) {
		System.out.println(allKmers);

	}

	/**
	 * Method for extracting the count of kmers in a whole genome
	 * @param genome the input genome
	 * @param patterns list of the patterns to apply
	 * @return map Kmer->count
	 * */
	public  Map<String, Integer> kmersCountsExtract(StringBuilder genome, List<String> patterns){

		HashMap<String, Integer> map = new HashMap<String, Integer>();
//		int count, k;
//		//int den = genome.length()-k+1;
//
//		if(Constant.DEBUG_MODE)
//			System.out.println("Length: "+ genome.length());
//
//		appoggio = new ArrayList<String>();
//		for(String pattern : patterns){
//			if(pattern.contains("0")||pattern.length()<3)
//				appoggio.add(pattern);
//			else{
//				String s1, s2;
//				s1=pattern.substring(0, pattern.length()-1);
//				s2=pattern.substring(1, pattern.length()-1);
//				if(!patterns.contains(s1))
//					appoggio.add(s1);
//				if(!patterns.contains(s2))
//					appoggio.add(s2);
//			}
//		}
//		Set s = new HashSet<String>(patterns);
//		Set a = new HashSet<String>(appoggio);
//		
//		s.addAll(a);
//		patterns = new ArrayList<String>(s);
//		
//		patterns.sort(String.CASE_INSENSITIVE_ORDER);
//		
//		for(String pattern : patterns){
//
//			if(Util.isValidSpacedWordPattern(pattern)){
//
//				k = pattern.length();
//
//				/* cycle over the length of String till k-mers of length, k, can still be made */
//				for(int i = 0; i< (genome.length()-k+1); i++){
//					/* output each k-mer of length k, from i to i+k in String*/
//
//					String kmer = genome.substring(i, i+k);
//					//System.out.println(kmer);
//
//					if(map.get(kmer)==null)
//						count=1;
//					else
//						count=map.get(kmer) + 1;
//
//					map.put(kmer, count);
//				}
//			}
//		}
//		//		if(Constant.DEBUG_MODE)
//		//			System.out.println("Counts: "+map);
		kmersCountsExtract(map, genome.toString(),null, patterns);
		return map;
	}

}