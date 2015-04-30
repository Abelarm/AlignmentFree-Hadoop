package hadoop.piarwisesimilarity;

import hadoop.DistanceCOWritable;
import hadoop.DistanceDoubleWritable;
import hadoop.DistanceGenericWritable;
import hadoop.HadoopUtil;
import hadoop.IdSeqsWritable;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;

import utility.Constant;
import distance.CoPhylogDistance;
import distance.ContextObject;
import distance.DistanceMeasure;


/**
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * @author Francesco Gaetano - email: f.gaetano90@gmail.com
 * @author Luigi Lomasto - email: luigilomasto@gmail.com 
 * 
 * @version 2.5
 * 
 * Date: February, 15 2015
 */
public class PairwiseSimilarityCombiner extends Reducer<IdSeqsWritable, DistanceDoubleWritable, IdSeqsWritable, DistanceDoubleWritable> {
	private final DistanceDoubleWritable dist = new DistanceDoubleWritable();
		private Counter countErrors;
	private Counter countFunctions;

	@Override
	protected void setup(
			Reducer<IdSeqsWritable, DistanceDoubleWritable, IdSeqsWritable, DistanceDoubleWritable>.Context context)
					throws IOException, InterruptedException {
		super.setup(context);

		countErrors = context.getCounter("PairwiseSimilarity Combiner","errors");
		countErrors.setValue(0);

		countFunctions = context.getCounter("PairwiseSimilarity Combiner","number of functions");
		countFunctions.setValue(0);
	}

	@Override
	public void reduce(IdSeqsWritable key, Iterable<DistanceDoubleWritable> values, Context context) throws IOException, InterruptedException {

		countFunctions.increment(1);

		try{
			int numEl=0;
			DistanceMeasure distance = HadoopUtil.getDistanceMeasure(context.getConfiguration(), key.getDistanceClass().toString());
			double result = distance.initDistance() ; //Puo' essere 0 per la somma o 1 per il prodotto a secondo del tipo di distanza usata.
			
				

				for (DistanceDoubleWritable value : values){
						numEl+=value.getCount().get();
						result = distance.distanceOperator(result,value.getDist().get());
						
						
					
					}
				dist.getDist().set(result);
				dist.getCount().set(numEl);
				context.write(key, dist);

				if(Constant.DEBUG_MODE)
					System.out.println("Pattern "+key.getPattern()+" d_"+distance+"("+key.getIdSeq1()+","+key.getIdSeq2()+")=" +dist.get());

			}
		
		catch(Exception e){
			e.printStackTrace();
			countErrors.increment(1);
		}
	}


}