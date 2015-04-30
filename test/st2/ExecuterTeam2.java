package test.st2;


import distance.*;
import sequential.ComputeDistancesSequentialDriver;
import utility.Util;
/**
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * 
 * @version 1.8
 * 
 * Date: January, 30 2015
 */
public class ExecuterTeam2 {

	public static void main(String[] args) {

		try {

			initialize(); //TODO

			//TODO Read from configuration.
			Class<DistanceMeasure>[] distanceClasses = new Class[]{CoPhylogDistance.class};

			int splitStrategy = 2;  
			//			splitStrategy==1 => FastaFileInputFormat.class
			//			splitStrategy==2 => FastaLineInputFormat.class

			/* HDFS ROOT */
			String hdfsHomeDir = "/home/user/Scrivania/workspace/AlignmentFreeHadoop/data/HDFS/HAFS/"; 
			//hdfsHomeDir = "/user/user/HAFS/"; //Usato nell'esecuzione sul cluster o in pseudodistribuita.


			/* LOCAL INPUT/OUTPUT DIRECTORIES AND FILES */
			String localPathPrefix = "/home/user/Scrivania/workspace/AlignmentFreeHadoop/";


			String localInputFiles = localPathPrefix + "data/example/simple.fasta";
			//String localInputFiles = "/home/user/Scrivania/algorithm/example_brucella";


			String localPatternsFile = localPathPrefix + "data/example/Patterns.txt";


			String sequentialOutputDir = localPathPrefix + "data/OutputSequential/";

			String hadoopOutputDir = localPathPrefix + "data/OutputHadoop/";

			Util.deleteTree(sequentialOutputDir);
			Util.deleteTree(hadoopOutputDir);


			setPaths(hdfsHomeDir, sequentialOutputDir, hadoopOutputDir);


			runSequential(localInputFiles, localPatternsFile, sequentialOutputDir, distanceClasses, splitStrategy); /* SEQUENTIAL */


		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	private static void initialize() {
		// TODO Auto-generated method stub

	}


	private static void setPaths(String hdfsHomeDir, String sequentialOutputDir, String hadoopOutputDir) {

		if(hdfsHomeDir==null || hdfsHomeDir.equals("") || hdfsHomeDir.equals("/"))
			hdfsHomeDir = "/u	private HashMap<String, Integer> lengthsMap;ser/user/HAFS/";

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
		ComputeDistancesSequentialDriver seq = new ComputeDistancesSequentialDriver(inputFiles, patternsFile, outputDir, distClasses, true, true, splitStrategy);
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

}