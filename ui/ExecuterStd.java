package ui;


import java.io.File;

import lengths.SequencesLengthDriver;
import distance.*;
import sequential.ComputeDistancesSequentialDriver;
import utility.Constant;
import utility.GenerateRandom;
import utility.Util;
import hadoop.CopyInput;
import history.hadoop.indexing.KmersDriverStd;
import history.hadoop.piarwisesimilarity.PairwiseSimilarityDriverStd;

/**
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * 
 * @version 2.0
 * 
 * Date: February, 3 2015
 */
public class ExecuterStd {

	public static void main(String[] args) {

		try {

			initialize(); //TODO

			//TODO Read from configuration.
			Class<DistanceMeasure>[] distanceClasses = new Class[]{SquaredEuclideanWithFrequencies.class};

			int splitStrategy = 1;  
			//			splitStrategy==1 => FastaFileInputFormat.class
			//			splitStrategy==2 => FastaLineInputFormat.class

			/* HDFS ROOT */
			String hdfsHomeDir = "/home/user/Scrivania/DATA_HAFS/HDFS/HAFS/";
			//hdfsHomeDir = "/user/user/HAFS/"; //Usato nell'esecuzione sul cluster o in pseudodistribuita.


			/* LOCAL INPUT/OUTPUT DIRECTORIES AND FILES */
			String localPathPrefix = "/home/user/Scrivania/DATA_HAFS/";
			//String localPathPrefix = "/home/user/Scrivania/workspace/AlignmentFreeHadoop/";


			String localInputFiles = localPathPrefix + "data/example/simple.fasta";
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



			/* SEQUENZIALE */

			runSequential(localInputFiles, localPatternsFile, sequentialOutputDir, distanceClasses, splitStrategy); /* SEQUENTIAL */



			System.out.println("---------------------------------------------------------");



			/* HADOOP */

			//Eventuale copia input sull'HDFS
			if(Constant.COPY_INPUT_ON_HDFS){
				copyInput(localInputFiles, localPatternsFile, hdfsHomeDir);
				System.out.println("---------------------------------------------------------");
			}

			runHadoop(localInputFiles, localPatternsFile, hdfsHomeDir, splitStrategy, distanceClasses, hadoopLocalOutputDir);

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


	private static void runSequential(String inputFiles, String patternsFile, String outputDir, Class<DistanceMeasure>[] distClasses, int splitStrategy){

		System.out.println("Sequential");
		long startTime = System.currentTimeMillis();
		//ComputeDistancesSequentialDriverFast seq = new ComputeDistancesSequentialDriverFast(inputFiles, patternsFile, outputDir, distClasses);
		//ComputeDistancesSequentialDriver seq = new ComputeDistancesSequentialDriver(inputFiles, patternsFile, outputDir, distClasses);
		ComputeDistancesSequentialDriver seq = new ComputeDistancesSequentialDriver(inputFiles, patternsFile, outputDir, distClasses, true, true,splitStrategy);
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
			Class<DistanceMeasure>[] distanceClasses, String hadoopOutputDir) throws Exception{

		runHadoop_StepI(localInputFiles, localPatternsFile, hdfsHomeDir, splitStrategy); /* HADOOP STEP I */

		System.out.println("---------------------------------------------------------");

		//		if(splitStrategy!=1){//Aggregazione delle lunghezze usando Hadoop.
		//			runHadoop_StepIB(hdfsHomeDir); /* HADOOP STEP I B */
		//			System.out.println("---------------------------------------------------------");
		//		}//Altrimenti faccio tutto nello Step II (senza usare Hadoop).

		runHadoop_StepII(hdfsHomeDir, distanceClasses, hadoopOutputDir, splitStrategy); /* HADOOP STEP II */

	}

	private static void runHadoop_StepI(String localInputFile, String patternsFiles, String hdfsHomeDir, int splitStrategy) throws Exception{

		System.out.println("Hadoop");
		System.out.println("k-mers extraction");
		long startTime = System.currentTimeMillis();
		KmersDriverStd kmers = new KmersDriverStd(localInputFile, patternsFiles, hdfsHomeDir, splitStrategy);
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
		PairwiseSimilarityDriverStd pwsim = new PairwiseSimilarityDriverStd(hdfsHomeDir, distanceClasses, outputLocalDir, splitStrategy);
		pwsim.start();
		long endTime = System.currentTimeMillis();
		System.out.println("Time: "+(endTime-startTime)+" ms");

	}


}