package utility;


/**
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * 
 * @version 1.9
 * 
 * Date:  February, 3 2015
 */
public class Constant {
	
	//TODO Indica di gestire tutto nella configurazione iniziale.

	/* HADOOP */

	public final static String HDFS_INPUT_DIR ="INPUT/";

	public final static String HDFS_OUTPUT_DIR ="OUTPUT/";

	public final static String HDFS_PATTERNS_FILE_HDFS = "Patterns.txt";

	public final static String HDFS_OUTPUT_DIR_I =HDFS_OUTPUT_DIR+"STEP_I";
	
	public final static String HDFS_OUTPUT_DIR_CHAR_COUNT =HDFS_OUTPUT_DIR+"CHAR_COUNT";


	public final static String HDFS_OUTPUT_DIR_II =HDFS_OUTPUT_DIR+"STEP_II";
	

	public final static String HDFS_DEBUG_DIR = HDFS_OUTPUT_DIR+"DEBUG";

	public final static String HDFS_DEBUG_DIR_STEP_I = HDFS_DEBUG_DIR+"/Kword_Seqs_Counts";

	public final static String HDFS_ID_SEQS_DIR = HDFS_OUTPUT_DIR+"/IdSeqsDir/";

	public final static String HDFS_ID_SEQS_FILE = HDFS_ID_SEQS_DIR+"/IdSeqs";

	public final static String HDFS_ALL_SEQS_IDS = HDFS_OUTPUT_DIR+"/IDs_Hadoop.txt";
	
	public final static String HDFS_ALL_SEQS_IDS_DIR = HDFS_OUTPUT_DIR+"/IDs_Hadoop";

	public final static String HDFS_DIST_FILE = HDFS_OUTPUT_DIR+"/Distances_Hadoop.txt";
	
	public static final String PROBABILITIES_PATH = "Probabilities.txt";


	/* LOCAL */ 

	public final static String LOCAL_OUTPUT_FILE_SEQ ="Distances_Sequential.txt";

	public final static String LOCAL_OUTPUT_FILE_HADOOP ="Distances_Hadoop.txt";

	public final static String LOCAL_OUTPUT_IDS_SEQ ="/IDs_Sequential.txt";

	public final static String LOCAL_OUTPUT_IDS_HADOOP ="/IDs_Hadoop.txt";



	/* OTHER */

	public final static String TEXT ="text";

	public final static String SEQ ="seq";
	
	public final static String SEQ2 ="seql1";


	public final static  char JOLLY_CHARACTER = '*';
	
	public final static String MARKED = "*";
	
	public static final String DNA_ALPHABET = "ACGT";

	public static final String PROTEIN_ALPHABET = "ARNDCQEGHILKMFPSTWYV";

	public static final int FASTA_MAX_LINE_LENGTH = 80;


	
	public final static  boolean CHECK_INPUT = true; //TODO

	public final static  boolean SPLIT2_DEBUG_MODE = false; //TODO

	public final static  boolean DEBUG_MODE = false; //TODO

	public final static boolean PRINT_MATRIX_SEQ = false; //TODO
	
	public final static boolean PRINT_AVG_DISTANCES = false;//TODO

	public final static  boolean HADOOP_PRINT_DISTANCES = false; //TODO

	public final static  boolean SEQUENTIAL_PRINT_DISTANCES = false; //TODO

	public final static boolean SET_REDUCE = true; //TODO

	public  static Integer NUM_REDUCER_JOB_1= 1; //TODO
	
	public  static Integer NUM_REDUCER_JOB_2= 1; //TODO
	
	public final static Integer BUFFER_CAPACITY_SEQ = 1048576; //TODO
	
	public final static boolean IS_PROTEIN = false;	//TODO


	public final static  boolean MERGE_DISTANCES_ON_HDFS = false; //TODO

	public final static  boolean MERGE_DISTANCE_ON_LOCAL_FS = true; //TODO

	public final static  boolean COPY_INPUT_ON_HDFS = false; //TODO
	
	public final static  boolean GENERATE_INPUT = false; //TODO

	public static final String LOCAL_PATH_PREFIX = "/Scrivania/TEAM4/";
	
}
