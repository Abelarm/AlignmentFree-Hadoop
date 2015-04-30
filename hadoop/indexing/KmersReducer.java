package hadoop.indexing;

import hadoop.ArrayKmerCOWritable;
import hadoop.ArrayKmerCountWritable;
import hadoop.KmerCOWritable;
import hadoop.KmerCountWritable;
import hadoop.KmerGenericWritable;

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

import utility.Constant;

/**
 * Reducer: Compute k-tuple distances between DNA sequences.
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * @author Francesco Gaetano - email: f.gaetano90@gmail.com
 * @author Luigi Lomasto - email: luigilomasto@gmail.com 
 * 
 * @version 1.5
 * 
 * Date: February, 15 2015.
 */
public class KmersReducer extends Reducer<Text, KmerGenericWritable, Text, Writable>{
	private final KmerGenericWritable kgw = new KmerGenericWritable();
	private final ArrayKmerCountWritable arrayCount = new ArrayKmerCountWritable();
	private final ArrayKmerCOWritable arrayCO = new ArrayKmerCOWritable();
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
	public void reduce(Text kword, Iterable<KmerGenericWritable> values, Context context) throws IOException, InterruptedException {

		//Se ci sono troppi dati in input si dovrà pensare a creare un nuovo job MR.

		countFunctions.increment(1);

		try{
			Map<String,Integer> mappaCount = new HashMap<String,Integer>();
			Map<String,String> mappaCO = new HashMap<String,String>();

			//Inizialmente devo aggregare i contatori dei kmers provenienti dalla stessa sequenza genomica.
			for (KmerGenericWritable val : values) {
				Writable w = val.get();
				if(w instanceof KmerCountWritable){
					
					KmerCountWritable v = (KmerCountWritable) w;

					String idSeq = v.getIdSeq().toString();

					Integer intero = mappaCount.get(idSeq);
					
						

					if(intero==null)
						mappaCount.put(idSeq, v.getCount().get());
					else
						mappaCount.put(idSeq, intero+v.getCount().get());
				}
				else if(w instanceof KmerCOWritable){
					KmerCOWritable v =  (KmerCOWritable) w;

					String v1 = mappaCO.get(v.getIdSeq().toString());

					if(v1!=null){
						if(!mappaCO.get(v.getIdSeq().toString()).equals(Constant.MARKED)){
							if(!v1.equals(v.getObject().toString()))
								mappaCO.put(v.getIdSeq().toString(), Constant.MARKED);
						}
					}
					else
						mappaCO.put(v.getIdSeq().toString(), v.getObject().toString());
						
					mappaCO.put(v.getIdSeq().toString(), v.getObject().toString());
				}
			}


			writeOutputCO(kword, mappaCO);
			writeOutputCount(kword, mappaCount);






		}
		catch(Exception e){
			e.printStackTrace();
			countErrors.increment(1);
		}
	}


	//	@Override
	//	public void reduce(Text kword, Iterable<KmerCountWritable> values, Context context) throws IOException, InterruptedException {
	//
	//		countFunctions.increment(1);
	//
	//		try{
	//
	//			ArrayList<KmerCountWritable> list = new ArrayList<KmerCountWritable>();  
	//
	//			for (KmerCountWritable val : values) {
	//
	//				KmerCountWritable c = new KmerCountWritable(val.getIdSeq().toString(), val.getCount().get()); //Clone dell'oggetto.
	//				list.add(c);
	//				//System.out.println(kword + " "+val);
	//			}
	//
	//			//System.out.println(kword + " "+ list+"\n");
	//
	//			//context.write(kword, new ArrayKmerCountWritable(list.toArray(new KmerCountWritable[list.size()])));
	//			array.set(list.toArray(new KmerCountWritable[list.size()]));
	//
	//			//context.write(kword, a);
	//			mos.write(Constant.SEQ, kword, array);
	//
	//			if(Constant.DEBUG_MODE)
	//				mos.write(Constant.TEXT, kword, new Text(array.toString()), HDFS_HOME_DIR+"/"+Constant.HDFS_DEBUG_DIR_STEP_I);
	//		}
	//		catch(Exception e){
	//			e.printStackTrace();
	//			countErrors.increment(1);
	//		}
	//	}

	private void writeOutputCount(Text kword, Map<String, Integer> mappaCount) throws IOException, InterruptedException {

		List<KmerCountWritable> listCount = new ArrayList<KmerCountWritable>();
		Iterator<Entry<String, Integer>> itCount = mappaCount.entrySet().iterator();

		//In output si restituisce il kmer e la lista delle sequenze genomiche in cui compare con i rispettivi contatori. 
		while(itCount.hasNext()){

			Entry<String, Integer> e = itCount.next();
			KmerCountWritable c = new KmerCountWritable(e.getKey(), e.getValue());
			listCount.add(c);

		}

		//context.write(kword, new ArrayKmerCountWritable(list.toArray(new KmerCountWritable[list.size()])));
		arrayCount.set(listCount.toArray(new KmerCountWritable[listCount.size()]));

		//context.write(kword, a);

		kgw.set(arrayCount);
		if(arrayCount.get().length!=0)

			if(kword.getLength()==1)
				mos.write(Constant.SEQ2, kword, kgw);
			
			
		

			mos.write(Constant.SEQ, kword, kgw);


		if(Constant.DEBUG_MODE && listCount.size()>0)
			mos.write(Constant.TEXT, kword, new Text(arrayCount.toString()), HDFS_HOME_DIR+"/"+Constant.HDFS_DEBUG_DIR_STEP_I);

		listCount.clear();		
	}

	
	
	private void writeOutputCO(Text kword, Map<String, String> mappaCO) throws IOException, InterruptedException {
		List<KmerCOWritable> listCO = new ArrayList<KmerCOWritable>();  

		Iterator<Entry<String, String>> itCO = mappaCO.entrySet().iterator();

		//In output si restituisce il kmer e la lista delle sequenze genomiche in cui compare con i rispettivi oggetti. 
		while(itCO.hasNext()){

			Entry<String, String> e = itCO.next();
			if(!e.getValue().equals(Constant.MARKED)){
				KmerCOWritable c = new KmerCOWritable(new Text(e.getKey()), new Text(e.getValue()));
				listCO.add(c);
			}

		}
		if(!listCO.isEmpty()){

			//context.write(kword, new ArrayKmerCountWritable(list.toArray(new KmerCountWritable[list.size()])));
			arrayCO.set(listCO.toArray(new KmerCOWritable[listCO.size()]));

			//context.write(kword, a);
			kgw.set(arrayCO);

			if(arrayCO.get().length!=0)
				mos.write(Constant.SEQ, kword, kgw);

			if(Constant.DEBUG_MODE && listCO.size()>0)
				mos.write(Constant.TEXT, kword, new Text(arrayCO.toString()), HDFS_HOME_DIR+"/"+Constant.HDFS_DEBUG_DIR_STEP_I);

			listCO.clear();	
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