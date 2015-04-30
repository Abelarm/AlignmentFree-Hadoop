package ui;


import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import lengths.SequencesLengthDriver;
import distance.*;
import distance.d2.*;
import sequential.ComputeDistancesSequentialDriver;
import utility.Constant;
import utility.GenerateRandom;
import utility.Util;
import utility.XMLConfigParser;
import hadoop.CopyInput;
import hadoop.CopyProbabilities;
import hadoop.HadoopUtil;
import hadoop.indexing.KmersDriver;
import hadoop.piarwisesimilarity.PairwiseSimilarityDriver;

/**
 * 
 * @author Steven Rosario Sirchia - Luigi Giugliano
 * 
 * @version 2.1
 * 
 * Date: February, 16 2015
 */
public class ExecuterMap {

	public static void main(String[] args) {

		try {

			
            Set<String> set=new HashSet<String>();
            
            for(String s: args){
            	set.add(s);
            	
            }
            if(set.size()<3)
            {
            	System.out.println("Aggiungi come argomenti nel seguente ordine:");
            	System.out.println("1) file di configurazione");
            	System.out.println("2) S1 o S2 a seconda dello Split");
            	/*System.out.println("4) S o H se in sequenziale o con Hadoop");
            	System.out.println("5) C se si vuole copiare sull'Hdfs");*/
            	System.out.println("6) Almeno uno dei due job J1 J2 (Anche entrambi)");
            	return;
            }
            
            HashMap<String, String> configMap = initialize(args[0]);
            
			//TODO Read from configuration.
			
			ArrayList<Class<DistanceMeasure>> classes = new ArrayList<Class<DistanceMeasure>>();
			String names = configMap.get("CLASS_NAMES");
			List<String> classNames = Arrays.asList(names.split(","));
			for(String distance : classNames){
				classes.add((Class<DistanceMeasure>) Class.forName(distance));
			}
			Class<DistanceMeasure>[] distanceClasses = new Class[classes.size()];
			classes.toArray(distanceClasses);
			
			int numReduce1=Integer.parseInt(configMap.get("NUM_REDUCER_JOB_1"));
			int numReduce2=Integer.parseInt(configMap.get("NUM_REDUCER_JOB_2"));
			Constant.NUM_REDUCER_JOB_1=numReduce1;
			Constant.NUM_REDUCER_JOB_2=numReduce2;
			
			int splitStrategy = Integer.parseInt(configMap.get("SPLIT_TYPE"));
           
            
            
			//			splitStrategy==1 => FastaFileInputFormat.class
			//			splitStrategy==2 => FastaLineInputFormat.class

			/* HDFS ROOT */
			//String hdfsHomeDir = "/home/user/Scrivania/DATA_HAFS/HDFS/HAFS/";
			//String hdfsHomeDir = "/home/user/Scrivania/workspace/AlignmentFreeHadoop/data/HDFS/HAFS/"; 
			//String hdfsHomeDir = "/home/user/Scrivania/workspace/AlignmentFreeHadoop/data/HDFS/HAFS/";
			String hdfsHomeDir = configMap.get("HDFS_HOME_DIR");
         

			/* LOCAL INPUT/OUTPUT DIRECTORIES AND FILES */
			//String localPathPrefix = "/home/user/Scrivania/DATA_HAFS/";
			//String localPathPrefix = "/home/user/Scrivania/workspace/AlignmentFreeHadoop/";
			
			//TODO
			String localHome = System.getProperty("user.home");
			
			//String localInputFiles = localPathPrefix + "data/example/dir/prova/";
			String localInputFiles = localHome+configMap.get("LOCAL_PATH_PREFIX")+configMap.get("LOCAL_INPUT_FILES");
			//String localInputFiles = "/home/user/Scrivania/big.fasta";


			String localPatternsFile = localHome+configMap.get("LOCAL_PATH_PREFIX")+configMap.get("LOCAL_PATTERNS_FILE");
			System.out.println(localPatternsFile);

			String sequentialOutputDir = localHome+configMap.get("LOCAL_PATH_PREFIX")+configMap.get("SEQUENTIAL_OUTPUT_DIR");

			String hadoopLocalOutputDir = localHome+configMap.get("LOCAL_PATH_PREFIX")+configMap.get("HADOOP_LOCAL_OUTPUT_DIR");

			boolean is_protein = Boolean.parseBoolean(configMap.get("IS_PROTEIN"));
			
			
			//TODO Verificare che se splitStrategy==2 allora ogni file deve contenere una sola sequenza genomica.

			Util.deleteTree(sequentialOutputDir);
			Util.deleteTree(hadoopLocalOutputDir);

			setPaths(hdfsHomeDir, sequentialOutputDir, hadoopLocalOutputDir);

			if(Boolean.parseBoolean(configMap.get("GENERATE_INPUT"))){
				int numSeq = Integer.parseInt(configMap.get("GENERATE_NUM_SEQ"));
				int avgSeqLength = Integer.parseInt(configMap.get("GENERATE_AVG_LENGTH"));
				int sdSeqLength = Integer.parseInt(configMap.get("GENERATE_SD_LENGTH"));
				generateInputFiles(is_protein, numSeq, avgSeqLength, sdSeqLength, localInputFiles); 
			}
			
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
			if(Boolean.parseBoolean(configMap.get("COPY_INPUT_ON_HDFS"))){
				copyInput(localInputFiles, localPatternsFile, hdfsHomeDir);
				System.out.println("---------------------------------------------------------");
			}
			
			for(Class<DistanceMeasure> c : distanceClasses){
				if(c.newInstance() instanceof FixedProb){
					String localProbFile = localHome+configMap.get("LOCAL_PATH_PREFIX")+configMap.get("PROBABILITIES_PATH");
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


	private static HashMap<String, String> initialize(String configFile) {
		return XMLConfigParser.readXML(configFile);
	}

	private static void generateInputFiles(boolean is_protein, int numSeq, int avgSeqLength, int sdSeqLength, String localInputFiles) {

		File f = new File(localInputFiles);

		if(is_protein)
			GenerateRandom.randomProteinFastaFile(numSeq, avgSeqLength, sdSeqLength, f);
		else
			GenerateRandom.randomDnaFastaFile(numSeq, avgSeqLength, sdSeqLength, f) ;
	}


	private static void copyInput(String localInputFiles, String localPatternsFile, String homeHdfs) throws Exception {
		
		System.out.println("Copia dell'Input sull'HDFS");
		long startTime = System.currentTimeMillis();
		CopyInput ci = new CopyInput(localInputFiles, localPatternsFile,  homeHdfs);
		ci.start();
		long endTime = System.currentTimeMillis();
		System.out.println("Time: "+(endTime-startTime) +" ms");

	}

	private static void copyProbs(String localProbFile, String homeHdfs) throws Exception {
		
		System.out.println("Copia delle Probabilità sull'HDFS");
		long startTime = System.currentTimeMillis();
		CopyProbabilities ci = new CopyProbabilities(localProbFile,  homeHdfs);
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


}