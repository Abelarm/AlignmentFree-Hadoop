package ui;


import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import lengths.SequencesLengthDriver;
import distance.*;
import distance.d2.ConcatEstimatedProbD2Star;
import distance.d2.D2;
import distance.d2.EstimatedProbD2S;
import distance.d2.EstimatedProbD2Star;
import distance.d2.FixedProb;
import distance.d2.FixedProbD2S;
import distance.d2.FixedProbD2Star;
import distance.d2.UniformProbD2S;
import distance.d2.UniformProbD2Star;
import sequential.ComputeDistancesSequentialDriver;
import utility.Constant;
import utility.GenerateRandom;
import utility.Util;
import hadoop.CopyInput;
import hadoop.CopyProbabilities;
import hadoop.indexing.KmersDriver;
import hadoop.piarwisesimilarity.PairwiseSimilarityDriver;

/**
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * 
 * @version 2.0
 * 
 * Date: February, 3 2015
 */
public class Executer {

	public static void main(String[] args) {

		try {

			initialize(); //TODO
            Set<String> set=new HashSet<String>();
            
            for(String s: args){
            	set.add(s);
            	
            }
            
            
            if(set.size()<5)
            {
            	System.out.println("Aggiungi come argomenti nel seguente ordine:");
            	System.out.println("1) #Reduce JOB 1 (richiesto)");
            	System.out.println("2) #Reduce JOB 2 (richiesto)");
            	System.out.println("3) S1 o S2 a seconda dello Split");
            	System.out.println("4) S o H se in sequenziale o con Hadoop");
            	System.out.println("5) C se si vuole copiare sull'Hdfs");
            	System.out.println("6) Almeno uno dei due job J1 J2 (Anche entrambi)");
            	return;
            }
			//TODO Read from configuration.
			//Class<DistanceMeasure>[] distanceClasses = new Class[]{CoPhylogDistance.class,JSDLog2WithFrequencies.class,JSDLog2.class,JSDLogNWithFrequencies.class, JSDLogN.class, SquaredEuclidean.class, D2.class, ChebychevWithFrequencies.class, Manhattan.class, ManhattanWithFrequencies.class, KLDLog2.class, KLDLogN.class };
            Class<DistanceMeasure>[] distanceClasses = new Class[]{CoPhylogDistance.class,JSDLog2WithFrequencies.class,JSDLog2.class,JSDLogNWithFrequencies.class, JSDLogN.class, Euclidean.class, SquaredEuclidean.class, ChebychevWithFrequencies.class, Manhattan.class, ManhattanWithFrequencies.class, KLDLog2.class, KLDLogN.class, D2.class, ConcatEstimatedProbD2Star.class, EstimatedProbD2Star.class, FixedProbD2Star.class, UniformProbD2Star.class, EstimatedProbD2S.class, FixedProbD2S.class, UniformProbD2S.class};
			System.out.println("Distances:");
			for(Class<DistanceMeasure> d:distanceClasses)
				System.out.println(d.getName());
			
			int numReduce1=Integer.parseInt(args[0]);
			int numReduce2=Integer.parseInt(args[1]);
			
			Constant.NUM_REDUCER_JOB_1=numReduce1;
			Constant.NUM_REDUCER_JOB_2=numReduce2;
			
			System.out.println("");
			System.out.println("Num reducers Job 1: "+numReduce1);
			System.out.println("Num reducers Job 2: "+numReduce2);			
			
			
			int splitStrategy;
            if(set.contains("S1"))
			    splitStrategy = 1;
            else
            	if(set.contains("S2"))
            		splitStrategy=2;
            	else{
            		System.out.println("Scegli lo split, 1 o 2");
            		return;
            		
            	}
            
            
			//			splitStrategy==1 => FastaFileInputFormat.class
			//			splitStrategy==2 => FastaLineInputFormat.class

			/* HDFS ROOT */
			//String hdfsHomeDir = "/home/user/Scrivania/DATA_HAFS/HDFS/HAFS/";
			//String hdfsHomeDir = "/home/user/Scrivania/workspace/AlignmentFreeHadoop/data/HDFS/HAFS/"; 
			//String hdfsHomeDir = "/home/user/Scrivania/workspace/AlignmentFreeHadoop/data/HDFS/HAFS/";
			String hdfsHomeDir = "/user/user/HAFS/"; //Usato nell'esecuziuita.
         

			/* LOCAL INPUT/OUTPUT DIRECTORIES AND FILES */
			//String localPathPrefix = "/home/user/Scrivania/DATA_HAFS/";
			//String localPathPrefix = "/home/user/Scrivania/workspace/AlignmentFreeHadoop/";
			
			//TODO
			//System.getProperty("user.home");

			//String localInputFiles = localPathPrefix + "data/example/dir/prova/";
			String localInputFiles = "/home/user/Scrivania/TEAM4/INPUT/Split/";
			//String localInputFiles = "/home/user/Scrivania/big.fasta";


			String localPatternsFile ="/home/user/Scrivania/TEAM4/Pattern/Patterns.txt";

           
			String sequentialOutputDir ="/home/user/Scrivania/TEAM4/OutputSequential/";

			String hadoopLocalOutputDir = "/home/user/Scrivania/TEAM4/OutputHadoop/";

			String alphabet = Constant.DNA_ALPHABET; //TODO read from configuration.
			
			int numSeq=0, avgSeqLength=0, sdSeqLength=0;  //TODO read from configuration.
			
			//TODO Verificare che se splitStrategy==2 allora ogni file deve contenere una sola sequenza genomica.

			Util.deleteTree(sequentialOutputDir);
			Util.deleteTree(hadoopLocalOutputDir);

			setPaths(hdfsHomeDir, sequentialOutputDir, hadoopLocalOutputDir);

			if(Constant.GENERATE_INPUT)
				generateInputFiles(alphabet, numSeq, avgSeqLength, sdSeqLength, localInputFiles); 

			
			DistanceMeasure[] distances = new DistanceMeasure[distanceClasses.length];
			
			for(int i=0; i<distances.length; i++){
				try {
					Object obj = distanceClasses[i].newInstance();

					if(obj instanceof DistanceMeasure)
						distances[i] = (DistanceMeasure) obj;

				} catch (Exception e) {
					e.printStackTrace();
				} 
			}
			boolean hasCO = Util.isContexObjectMeasure(distances);
			boolean hasCount=false;
			if((distances.length>1 && hasCO) || (distances.length>0 && !hasCO)){
				hasCount=true;	
				
			}
			


			/* SEQUENZIALE */
            if(set.contains("S"))
			runSequential(localInputFiles, localPatternsFile, sequentialOutputDir, distanceClasses, hasCO, hasCount, splitStrategy); /* SEQUENTIAL */



			System.out.println("---------------------------------------------------------");

          

			/* HADOOP */

			//Eventuale copia input sull'HDFS
			if(set.contains("C")){
				copyInput(localInputFiles, localPatternsFile, hdfsHomeDir);
				System.out.println("---------------------------------------------------------");
			}
			
			for(Class<DistanceMeasure> c : distanceClasses){
				if(c.newInstance() instanceof FixedProb && set.contains("C")){
					System.out.println(c.newInstance().toString());
					String localProbFile = System.getProperty("user.home")+Constant.LOCAL_PATH_PREFIX+Constant.PROBABILITIES_PATH;
					System.out.println(localProbFile);
					copyProbs(localProbFile, hdfsHomeDir);
					break;
				}
			}
			
			if(set.contains("H"))
			runHadoop(localInputFiles, localPatternsFile, hdfsHomeDir, splitStrategy, distanceClasses, hadoopLocalOutputDir, hasCO, hasCount,set);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	private static void initialize() {
		// TODO Auto-generated method stub

	}

	private static void generateInputFiles(String alphabet, int numSeq, int avgSeqLength, int sdSeqLength, String localInputFiles) {

		File f = new File(localInputFiles);

		if(alphabet.equals(Constant.DNA_ALPHABET))
			GenerateRandom.randomDnaFastaFile(numSeq, avgSeqLength, sdSeqLength, f) ;
		else
			if(alphabet.equals(Constant.PROTEIN_ALPHABET))
				GenerateRandom.randomProteinFastaFile(numSeq, avgSeqLength, sdSeqLength, f);
	}


	private static void copyInput(String localInputFiles, String localPatternsFile, String homeHdfs) throws Exception {

		System.out.println("Copia dell'Input sull'HDFS");
		long startTime = System.currentTimeMillis();
		CopyInput ci = new CopyInput(localInputFiles, localPatternsFile,  homeHdfs);
		ci.start();
		long endTime = System.currentTimeMillis();
		System.out.println("Time: "+(endTime-startTime) +" ms");

	}


	private static void setPaths(String hdfsHomeDir, String sequentialOutputDir, String hadoopOutputDir) {

		if(hdfsHomeDir==null || hdfsHomeDir.equals("") || hdfsHomeDir.equals("/"))
			hdfsHomeDir = "/user/user/HAFS/";

		if(sequentialOutputDir==null || sequentialOutputDir.equals("") || sequentialOutputDir.equals("/"))
			sequentialOutputDir = "/home/user/HAFS/OutputSequential/";

		if(hadoopOutputDir==null || hadoopOutputDir.equals("") || hadoopOutputDir.equals("/"))
			hadoopOutputDir = "/home/user/HAFS/OutputHadoop/";

	}


	private static void runSequential(String inputFiles, String patternsFile, String outputDir, Class<DistanceMeasure>[] distClasses, boolean hasCO, boolean hasCount, int splitStrategy){

		System.out.println("Sequential");
		long startTime = System.currentTimeMillis();
		//ComputeDistancesSequentialDriverFast seq = new ComputeDistancesSequentialDriverFast(inputFiles, patternsFile, outputDir, distClasses);
		ComputeDistancesSequentialDriver seq = new ComputeDistancesSequentialDriver(inputFiles, patternsFile, outputDir, distClasses, hasCO, hasCount,splitStrategy);
		seq.start(); 
		long endTime = System.currentTimeMillis();
		System.out.println("Time: "+(endTime-startTime) +" ms");

		//		System.out.println("Sequential - alternative");
		//		startTime = System.currentTimeMillis();
		//		ComputeDistancesDriverOld.main(args);
		//		endTime = System.currentTimeMillis();
		//		System.out.println("Time: "+(endTime-startTime) +" ms");
		//		System.out.println("---------------------------------------------------------");

		
	}

	private static void runHadoop(String localInputFiles, String localPatternsFile, String hdfsHomeDir, int splitStrategy,
			Class<DistanceMeasure>[] distanceClasses, String hadoopOutputDir, boolean hasCO, boolean hasCount, Set<String> set) throws Exception{
        
		if(set.contains("J1"))
		runHadoop_StepI(localInputFiles, localPatternsFile, hdfsHomeDir, splitStrategy, hasCO, hasCount,distanceClasses); /* HADOOP STEP I */

		System.out.println("---------------------------------------------------------");

		//		if(splitStrategy!=1){//Aggregazione delle lunghezze usando Hadoop.
		//			runHadoop_StepIB(hdfsHomeDir); /* HADOOP STEP I B */
		//			System.out.println("---------------------------------------------------------");
		//		}//Altrimenti faccio tutto nello Step II (senza usare Hadoop).

		if(set.contains("J2"))
		runHadoop_StepII(hdfsHomeDir, distanceClasses, hadoopOutputDir, splitStrategy); /* HADOOP STEP II */

	}

	private static void runHadoop_StepI(String localInputFile, String patternsFiles, String hdfsHomeDir, int splitStrategy, boolean hasCO, boolean hasCount, Class<DistanceMeasure>[] distClasses) throws Exception{

		System.out.println("Hadoop");
		System.out.println("k-mers extraction");
		System.out.println("Patterns utilizzati: ");
		printPatterns(patternsFiles);
		System.out.println("Num reducer S1: "+Constant.NUM_REDUCER_JOB_1+"\tNum reducer Job2: "+Constant.NUM_REDUCER_JOB_2);

		long startTime = System.currentTimeMillis();
		KmersDriver kmers = new KmersDriver(hdfsHomeDir, splitStrategy, hasCO, hasCount,distClasses);
		kmers.start();
		long endTime = System.currentTimeMillis();
		System.out.println("Time: "+(endTime-startTime)+" ms");
 
	}


	private static void runHadoop_StepIB(String hdfsHomeDir) throws Exception{

		System.out.println("\n\nSequences Length:");
		long startTime = System.currentTimeMillis();
		SequencesLengthDriver sldriver = new SequencesLengthDriver(hdfsHomeDir);
		sldriver.start();
		long endTime = System.currentTimeMillis();
		System.out.println("Time: "+(endTime-startTime)+" ms");

	}


	private static void runHadoop_StepII(String hdfsHomeDir, Class<DistanceMeasure>[] distanceClasses, String outputLocalDir, int splitStrategy) throws Exception{

		System.out.println("\n\nPairwise Similarity:");
		long startTime = System.currentTimeMillis();
		PairwiseSimilarityDriver pwsim = new PairwiseSimilarityDriver(hdfsHomeDir, distanceClasses, outputLocalDir, splitStrategy);
		pwsim.start();
		long endTime = System.currentTimeMillis();
		System.out.println("Time: "+(endTime-startTime)+" ms");

	}

private static void copyProbs(String localProbFile, String homeHdfs) throws Exception {
		
		System.out.println("Copia delle Probabilità sull'HDFS");
		long startTime = System.currentTimeMillis();
		CopyProbabilities ci = new CopyProbabilities(localProbFile,  homeHdfs);
		ci.start();
		long endTime = System.currentTimeMillis();
		System.out.println("Time: "+(endTime-startTime) +" ms");

	}
private static void printPatterns(String filename) {


	try {
		Scanner in = new Scanner(new File(filename));

		while(in.hasNextLine())
			System.out.println(in.nextLine());

		in.close();

	} catch (Exception e) {
		e.printStackTrace();
	}

}
}