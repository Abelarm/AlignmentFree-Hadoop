package ui;


import java.io.File;

import lengths.SequencesLengthDriver;
import distance.*;
import distance.d2.D2;
import distance.d2.EstimatedProbD2Star;
import sequential.ComputeDistancesSequentialDriver;
import utility.Constant;
import utility.GenerateRandom;
import utility.Util;
import hadoop.CopyInput;
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
public class ExecuterTeam2 {

	public static void main(String[] args) {

		try {

			initialize(); //TODO

			//TODO Read from configuration.
			Class<DistanceMeasure>[] distanceClasses = new Class[]{CoPhylogDistance.class};

			int splitStrategy = 1;  
			//			splitStrategy==1 => FastaFileInputFormat.class
			//			splitStrategy==2 => FastaLineInputFormat.class

			/* HDFS ROOT */
			//String hdfsHomeDir = "/home/user/Scrivania/DATA_HAFS/HDFS/HAFS/";
			String hdfsHomeDir = "/home/user/Scrivania/workspace/AlignmentFreeHadoop/data/HDFS/HAFS/"; 
			//hdfsHomeDir = "/user/user/HAFS/"; //Usato nell'esecuzione sul cluster o in pseudodistribuita.


			/* LOCAL INPUT/OUTPUT DIRECTORIES AND FILES */
			//String localPathPrefix = "/home/user/Scrivania/DATA_HAFS/";
			String localPathPrefix = "/home/user/Scrivania/workspace/AlignmentFreeHadoop/";
			
			//TODO
			//System.getProperty("user.home");

			String localInputFiles = localPathPrefix + "data/example/Seq2.fasta";
			//String localInputFiles = "/home/user/Scrivania/big.fasta";


			String localPatternsFile = localPathPrefix + "data/example/Patterns.txt";


			String sequentialOutputDir = localPathPrefix + "data/OutputSequential/";

			String hadoopLocalOutputDir = localPathPrefix + "data/OutputHadoop/";

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

			runSequential(localInputFiles, localPatternsFile, sequentialOutputDir, distanceClasses, hasCO, hasCount, splitStrategy); /* SEQUENTIAL */



			System.out.println("---------------------------------------------------------");



			/* HADOOP */

			//Eventuale copia input sull'HDFS
			if(Constant.COPY_INPUT_ON_HDFS){
				copyInput(localInputFiles, localPatternsFile, hdfsHomeDir);
				System.out.println("---------------------------------------------------------");
			}
			
			
			
			runHadoop(localInputFiles, localPatternsFile, hdfsHomeDir, splitStrategy, distanceClasses, hadoopLocalOutputDir, hasCO, hasCount);

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
			Class<DistanceMeasure>[] distanceClasses, String hadoopOutputDir, boolean hasCO, boolean hasCount) throws Exception{

		runHadoop_StepI(localInputFiles, localPatternsFile, hdfsHomeDir, splitStrategy, hasCO, hasCount, distanceClasses); /* HADOOP STEP I */

		System.out.println("---------------------------------------------------------");

		//		if(splitStrategy!=1){//Aggregazione delle lunghezze usando Hadoop.
		//			runHadoop_StepIB(hdfsHomeDir); /* HADOOP STEP I B */
		//			System.out.println("---------------------------------------------------------");
		//		}//Altrimenti faccio tutto nello Step II (senza usare Hadoop).

		runHadoop_StepII(hdfsHomeDir, distanceClasses, hadoopOutputDir, splitStrategy); /* HADOOP STEP II */

	}

	private static void runHadoop_StepI(String localInputFile, String patternsFiles, String hdfsHomeDir, int splitStrategy, boolean hasCO, boolean hasCount, Class<DistanceMeasure>[] distanceClasses) throws Exception{

		System.out.println("Hadoop");
		System.out.println("k-mers extraction");
		long startTime = System.currentTimeMillis();
		//KmersDriver kmers = new KmersDriver(hdfsHomeDir, splitStrategy, hasCO, hasCount);
		KmersDriver kmers = new KmersDriver(hdfsHomeDir, splitStrategy, hasCO, hasCount, distanceClasses);
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


}