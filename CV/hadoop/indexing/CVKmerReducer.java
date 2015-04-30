package CV.hadoop.indexing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import CV.hadoop.ArrayKmer4CountsWritable;
import CV.hadoop.Kmer4CountsWritable;
import utility.Constant;

/**
 * Reducer: Compute k, k-1 and k-2-tuple counts of a sequence, according to CV.
 * 
 * @author Luigi Giugliano - Steven Rosario Sirchia
 * 
 * @version 1.5
 * 
 * Date: February, 16 2015
 */
public class CVKmerReducer extends Reducer<Text, Kmer4CountsWritable, Text, Writable>{//Reducer<Text, KmerCountWritable, Text, ArrayKmerCountWritable> {

	private final ArrayKmer4CountsWritable array = new ArrayKmer4CountsWritable();
	//private final ArrayList<KmerMultiDoubleCountWritable> list = new ArrayList<KmerMultiDoubleCountWritable>();
	private MultipleOutputs<Text, Writable> mos;
	private Counter countErrors;
	private Counter countFunctions;
	private String HDFS_HOME_DIR;

	public void setup(Context context) {
		mos = new MultipleOutputs<Text, Writable>(context);

		HDFS_HOME_DIR = context.getConfiguration().get("HDFS_HOME_DIR");
		countErrors = context.getCounter("Kmers Reducer","errors");
		countErrors.setValue(0);

		countFunctions = context.getCounter("Kmers Reducer","number of functions");
		countFunctions.setValue(0);
	}

	@Override
	public void reduce(Text kword, Iterable<Kmer4CountsWritable> values, Context context) throws IOException, InterruptedException {

		//Se ci sono troppi dati in input si dovrà pensare a creare un nuovo job MR.

		countFunctions.increment(1);

		try{
			Map<String,Double> mappa = new HashMap<String,Double>();
			Map<String,ArrayList<Double>> C_Map = new HashMap<String,ArrayList<Double>>();
			Double c0, c1, c1b, c2,ct0, ct1, ct1b, ct2, p, p0, p1, p1b, p2, a;
			
			//Inizialmente devo aggregare i contatori dei kmers provenienti dalla stessa sequenza genomica.
			for (Kmer4CountsWritable val : values) {

				String idSeq = val.getIdSeq().toString();

				//Integer intero = mappa.get(idSeq);
				
				
				c0=val.getCount0().get();
				c1=val.getCount1().get();
				c1b=val.getCount1b().get();
				c2=val.getCount2().get();
				
				ArrayList<Double> temp= C_Map.get(idSeq);
				if(temp==null){
					temp= new ArrayList<Double>();
					temp.add(0, c0);
					temp.add(1, c1);
					temp.add(2, c1b);
					temp.add(3, c2);
					C_Map.put(idSeq, temp);
				}else{
					ct0=temp.get(0)+c0;
					ct1=temp.get(1)+c1;
					ct1b=temp.get(2)+c1b;
					ct2=temp.get(3)+c2;
					temp.clear();
					temp.add(0, ct0);
					temp.add(1, ct1);
					temp.add(2, ct1b);
					temp.add(3, ct2);
					C_Map.put(idSeq, temp);
					
				}
				
			}

			//System.out.println("Stampo C_MAP : "+C_Map);
			
			List<Kmer4CountsWritable> listAllC = new ArrayList<Kmer4CountsWritable>();  

			Iterator<Entry<String, ArrayList<Double>>> it2 = C_Map.entrySet().iterator();

			//In output si restituisce il kmer e la lista delle sequenze genomiche in cui compare con i rispettivi contatori. 
			while(it2.hasNext()){
				
				Entry<String, ArrayList<Double>> e = it2.next();
				Kmer4CountsWritable c = new Kmer4CountsWritable(e.getKey(), e.getValue().get(0), e.getValue().get(1), e.getValue().get(2), e.getValue().get(3));
				listAllC.add(c);
				
				//if(Constant.DEBUG_MODE)
					//System.out.println(kword+" "+c);
			}

			//System.out.println("QUI "+kword + " "+ listAllC+"\n");

			//context.write(kword, new ArrayKmerCountWritable(list.toArray(new KmerCountWritable[list.size()])));
			array.set(listAllC.toArray(new Kmer4CountsWritable[listAllC.size()]));

			//context.write(kword, a);
			mos.write(Constant.SEQ, kword, array);

			if(Constant.DEBUG_MODE)
				mos.write(Constant.TEXT, kword, new Text(array.toString()), HDFS_HOME_DIR+"/"+Constant.HDFS_DEBUG_DIR_STEP_I);
			
		}
		catch(Exception e){
			e.printStackTrace();
			countErrors.increment(1);
		}
	}

	public void cleanup(Context context) throws IOException {
		try {
			mos.close();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}