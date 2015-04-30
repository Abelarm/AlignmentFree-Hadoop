package lengths;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

/**
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * 
 * @version 1.0
 * 
 * Date: January, 30 2015
 */

public  class SequencesLengthReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private Counter countErrors;
	private Counter countFunctions;
	
	@Override
	protected void setup(
			Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		super.setup(context);
		
		countErrors = context.getCounter("SequencesLength Reducer","errors");
		countErrors.setValue(0);

		countFunctions = context.getCounter("SequencesLength Reducer","number of functions");
		countFunctions.setValue(0);
	}
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		countFunctions.increment(1);

		try{
		
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}

			context.write(key, new IntWritable(sum));

		}
		catch(Exception e){
			e.printStackTrace();
			countErrors.increment(1);
		}
		
	}

} 

