package CV.hadoop.indexing;

import hadoop.HadoopUtil;
import hadoop.inputsplit.ValueWritable;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import CV.hadoop.Kmer4CountsWritable;
import CV.hadoop.KmerSeqCV;
import utility.Constant;
import utility.Util;

/**
 * Mapper: Compute k, k-1 and k-2-tuple counts of a sequence, according to CV.
 * 
 * @author Luigi Giugliano - Steven Rosario Sirchia
 * 
 * @version 1.6
 * 
 * Date: February, 16 2015
 */
public class CVKmerMapper extends Mapper<Text, Writable, Text, Writable> {//extends Mapper<Text, Text, Text, KmerCountWritable> {

	private final Text kword = new Text();
	private final Text id_Seq = new Text();
	//private final IntWritable intero = new IntWritable();
	private final DoubleWritable doublec0 = new DoubleWritable();
	private final DoubleWritable doublec1 = new DoubleWritable();
	private final DoubleWritable doublec1b = new DoubleWritable();
	private final DoubleWritable doublec2 = new DoubleWritable();
	//private final KmerCountWritable kcw = new KmerCountWritable();
	private final Kmer4CountsWritable kcw = new Kmer4CountsWritable();
	private final HashMap<KmerSeqCV, Integer> map = new HashMap<KmerSeqCV, Integer>();
	private final HashMap<KmerSeqCV, ArrayList<Double>> mapoutput = new HashMap<KmerSeqCV, ArrayList<Double>>();
	private MultipleOutputs<Text, Writable> mos;
	private Counter countErrors;
	private Counter countFunctions;
	private Counter countGenomeStrings;
	private List<String> patterns;
	private int k;
	private String HDFS_HOME_DIR;
	private Text idSequence;
	//private Log LOG = LogFactory.getLog(KmersMapper.class);
	private int length=0;
	private ArrayList<String> appoggio;
	
	@Override
	protected void setup(Context context) throws IOException ,InterruptedException {
		super.setup(context);

		mos = new MultipleOutputs<Text, Writable>(context);

		context.getConfiguration().set("errors", "hasOccured");
		HDFS_HOME_DIR = context.getConfiguration().get("HDFS_HOME_DIR");

		countErrors = context.getCounter("Kmers Mapper","errors");
		countErrors.setValue(0);

		countFunctions = context.getCounter("Kmers Mapper","number of functions");
		countFunctions.setValue(0);

		countGenomeStrings = context.getCounter("Kmers Mapper","number of genome strings");
		countGenomeStrings.setValue(0);


		FileSystem fs = FileSystem.get(context.getConfiguration());

		URI[] localPaths = context.getCacheFiles();

		if(localPaths.length>0){
			patterns = HadoopUtil.readPatterns(fs, new Path(localPaths[0]));

			if(Constant.DEBUG_MODE)
				System.out.println("Patterns: "+patterns);
		}


		//System.out.println("K="+context.getConfiguration().get("K"));
	}


	//	@Override
	//	public void map(Text idSeq, Text allGenome, Context context) throws IOException, InterruptedException {
	//
	//		try{
	//			
	//			countFunctions.increment(1);
	//			
	//			String allStrGenome = allGenome.toString().trim();
	//			
	//			if(allStrGenome.equals("") || !Util.isValidFASTAFormat(allStrGenome))
	//				return;
	//			
	//			countGenomeStrings.increment(1);
	//
	//			if(Constant.DEBUG_MODE){
	//				String filename= ((FileSplit)context.getInputSplit()).getPath().getName();
	//				System.out.println("Filename: "+filename+": <"+idSeq+","+allStrGenome+">");
	//			}
	//			
	//			mos.write(Constant.TEXT, idSeq, allStrGenome.length(), HDFS_HOME_DIR+Constant.HDFS_ID_SEQS_FILE);
	//			
	//			for(String pattern : patterns){
	//				
	//				k = pattern.length();
	//				
	//				/* cycle over the length of String till k-mers of length, k, can still be made */
	//				for(int i = 0; i< (allStrGenome.length()-k+1); i++){
	//					/* output each k-mer of length k, from i to i+k in String*/
	//
	//					String kmer = allStrGenome.substring(i, i+k);
	//
	//					incrementValue(kmer, idSeq.toString(), pattern);
	//
	//				}	
	//			}
	//		}
	//		catch(Exception e){
	//			e.printStackTrace();
	//			countErrors.increment(1);
	//		}
	//	}

	@Override
	public void map(Text idSeq, Writable genome, Context context) throws IOException, InterruptedException {

		countFunctions.increment(1);
		context.progress();

		if(genome instanceof Text)
			computeAllGenome(idSeq, (Text) genome, context);
		else if(genome instanceof ValueWritable)
			computePartialGenome(idSeq, (ValueWritable) genome, context);
		
		context.progress();

	}



	public void computeAllGenome(Text idSeq, Text allGenome, Context context) throws IOException, InterruptedException {
		try{
			String kmer;
			String allStrGenome = allGenome.toString().trim();
			length=allStrGenome.length();
			
			if(allStrGenome.equals("") || !Util.isValidFASTAFormat(allStrGenome))
				return;

			countGenomeStrings.increment(1);

			if(Constant.DEBUG_MODE){
				String filename= ((FileSplit)context.getInputSplit()).getPath().getName();
				System.out.println("Filename: "+filename+": <"+idSeq+","+allStrGenome+">");
			}

			
			mos.write(Constant.TEXT, idSeq, allStrGenome.length(), HDFS_HOME_DIR+Constant.HDFS_ID_SEQS_FILE);
			
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
			
			HashSet<String> s = new HashSet<String>(patterns);
			HashSet<String> a = new HashSet<String>(appoggio);
			
			s.addAll(a);
			patterns = new ArrayList<String>(s);
			
			patterns.sort(String.CASE_INSENSITIVE_ORDER);
			for(String pattern : patterns){

				k = pattern.length();

				/* cycle over the length of String till k-mers of length, k, can still be made */
				for(int i = 0; i< (allStrGenome.length()-k+1); i++){
					/* output each k-mer of length k, from i to i+k in String*/

					kmer = allStrGenome.substring(i, i+k);

					incrementValue(kmer, idSeq.toString(), pattern);
				}	
			}
			
			patterns.removeAll(appoggio);
			for(String pattern : patterns){
				k = pattern.length();
				Iterator<Entry<KmerSeqCV,Integer>> it= map.entrySet().iterator();
				Entry<KmerSeqCV,Integer> val;
				String kmer1,kmer1b,kmer2;
				double c0,c1,c1b,c2;
				while(it.hasNext()){
					val=it.next();
					kmer=val.getKey().getKmer();
					if(kmer.length()!=k || kmer.length()<3 || kmer.contains("*"))
						continue;
					String idseq=val.getKey().getIdSeq();
					kmer1=kmer.substring(0, k-1);
					kmer1b=kmer.substring(1, k);
					kmer2=kmer.substring(1, k-1);

					c0=val.getValue();

					KmerSeqCV newkey = new KmerSeqCV(kmer1,idseq);
					if(map.containsKey(newkey)){
						c1=map.get(newkey);
					}
					else
						c1=0;

					newkey.setIdSeq(idseq);
					newkey.setKmer(kmer1b);
					if(map.containsKey(newkey)){
						c1b=map.get(newkey);
					}
					else
						c1b=0;

					newkey.setIdSeq(idseq);
					newkey.setKmer(kmer2);
					if(map.containsKey(newkey)){
						c2=map.get(newkey);
					}
					else
						c2=0;

					ArrayList<Double> allC = new ArrayList<Double>();
					allC.add(c0);
					allC.add(c1);
					allC.add(c1b);
					allC.add(c2);
					mapoutput.put(val.getKey(), allC);
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
			countErrors.increment(1);
		}
	}


	/*
	 * In questo caso ogni file di input è molto grande e contiene una sola sequenza genomica.
	 * Tutte le righe del file sono lunghe Constant.FASTA_MAX_LINE_LENGTH=80 caratteri tranne la prima (id delle sequenza).
	 * L'ultima riga può contenere meno di Constant.FASTA_MAX_LINE_LENGTH.
	 * Nel nostro caso la dimensione del pattern k deve essere minore o uguale a Constant.FASTA_MAX_LINE_LENGTH.
	 */
	public void computePartialGenome(Text idSeq, ValueWritable value, Context context) throws IOException, InterruptedException {

		try{
			int size=0;
			String genomeStr, kmer, next=null;

			genomeStr = value.getCurrLine().toString().trim();

			if(value.getNextLine()!=null)
				next = value.getNextLine().toString().trim();

			if(!Util.isValidFASTAFormat(genomeStr)) 
				return;

			if(next!=null && !Util.isValidFASTAFormat(next)) 
				return;

			countGenomeStrings.increment(1);

			//In questo caso un file contiene una sola sequenza e questi record sono relativi tutti alla stessa sequenza.
			if(idSequence==null)
				idSequence = new Text(idSeq.toString()); 
			length+=genomeStr.length();
			//Prima: mos.write(Constant.TEXT, idSeq, genomeStr.length(), HDFS_HOME_DIR+Constant.HDFS_ID_SEQS_FILE);
			
			
			if(Constant.DEBUG_MODE){
				String filename= ((FileSplit)context.getInputSplit()).getPath().getName();
				System.out.println("Filename: "+filename+": <"+idSeq+", ("+genomeStr+","+next+")>");
			}

			if(next!=null){
				size = genomeStr.length();
				genomeStr+=next;
			}	
			
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
			
			HashSet<String> s = new HashSet<String>(patterns);
			HashSet<String> a = new HashSet<String>(appoggio);
			
			s.addAll(a);
			patterns = new ArrayList<String>(s);
			
			patterns.sort(String.CASE_INSENSITIVE_ORDER);
			
			for(String pattern : patterns){

				k = pattern.length();
				
				if(next==null)// Ultima linea del file di input.
					size = genomeStr.length() - k + 1;
					
				/* cycle over the length of String till k-mers of length, k, can still be made */
				for(int i=0; i<size; i++){
					/* output each k-mer of length k, from i to i+k in String*/
					
					//Devo gestire il problema relativo all'eventuale lunghezza minore dell'ultima riga.
					if(genomeStr.substring(i).length()>=k){
						kmer = genomeStr.substring(i, i+k);
						incrementValue(kmer, idSeq.toString(), pattern);
					}
				}	
			}
			
			patterns.removeAll(appoggio);
			for(String pattern : patterns){
				k = pattern.length();

				Iterator<Entry<KmerSeqCV,Integer>> it= map.entrySet().iterator();
				Entry<KmerSeqCV,Integer> val;
				String kmer1,kmer1b,kmer2;
				double c0,c1,c1b,c2;
				while(it.hasNext()){
					val=it.next();
					kmer=val.getKey().getKmer();

					if(kmer.length()!=k || kmer.length()<3 || kmer.contains("*"))
						continue;
					
					String idseq=val.getKey().getIdSeq();
					kmer1=kmer.substring(0, k-1);
					kmer1b=kmer.substring(1, k);
					kmer2=kmer.substring(1, k-1);

					c0=val.getValue();

					KmerSeqCV newkey = new KmerSeqCV(kmer1,idseq);
					if(map.containsKey(newkey)){
						c1=map.get(newkey);
					}
					else
						c1=0;

					newkey.setIdSeq(idseq);
					newkey.setKmer(kmer1b);
					if(map.containsKey(newkey)){
						c1b=map.get(newkey);
					}
					else
						c1b=0;

					newkey.setIdSeq(idseq);
					newkey.setKmer(kmer2);
					if(map.containsKey(newkey)){
						c2=map.get(newkey);
					}
					else
						c2=0;

					ArrayList<Double> allC = new ArrayList<Double>();
					allC.add(c0);
					allC.add(c1);
					allC.add(c1b);
					allC.add(c2);
					mapoutput.put(val.getKey(), allC);
				}
			}
			/*if(Constant.DEBUG_MODE)
				System.out.println("Qui mapper "+mapoutput);*/
		}
		catch(Exception e){
			e.printStackTrace();
			countErrors.increment(1);
		}

	}




	private void incrementValue(String kmer, String idSeq, String pattern) {

		KmerSeqCV mapKey;

		if(pattern.contains("0")){
			String spacedWord = Util.extractSpacedWord(kmer, pattern); //kmer deve essere una spaced-word (inexact match).
			mapKey = new KmerSeqCV(spacedWord, idSeq);
		}
		else //kmer contiene tutti 1 (ossia e' un exact match).
			mapKey = new KmerSeqCV(kmer, idSeq);

		Integer mapValue = map.get(mapKey); 

		if(mapValue==null)
			map.put(mapKey, new Integer(1));
		else
			map.put(mapKey, mapValue + 1);

	}



	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

		super.cleanup(context);

		Iterator<Entry<KmerSeqCV, ArrayList<Double>>> it = mapoutput.entrySet().iterator();
		Entry<KmerSeqCV, ArrayList<Double>> kv;

		while(it.hasNext()){
			kv = it.next();
			//System.out.println(kv);

			kword.set(kv.getKey().getKmer());
			id_Seq.set(kv.getKey().getIdSeq());
			doublec0.set(kv.getValue().get(0));
			doublec1.set(kv.getValue().get(1));
			doublec1b.set(kv.getValue().get(2));
			doublec2.set(kv.getValue().get(3));

			
			kcw.setIdSeq(id_Seq);
			kcw.setCount0(doublec0);
			kcw.setCount1(doublec1);
			kcw.setCount1b(doublec1b);
			kcw.setCount2(doublec2);


			if(Constant.DEBUG_MODE)
				System.out.println("QUI MAPPER "+kword+" "+kcw);

			context.write(kword,kcw);
		}

		map.clear();
		mapoutput.clear();
		
		if(idSequence!=null && length>0)
			mos.write(Constant.TEXT, idSequence, length, HDFS_HOME_DIR+Constant.HDFS_ID_SEQS_FILE);

		try {
			mos.close();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

}
