package hadoop;

import org.apache.hadoop.io.ArrayWritable;

/**
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * 
 * @version 1.1
 * 
 * Date: January, 18 2015
 */
public class ArrayKmerCOWritable extends ArrayWritable{

	public ArrayKmerCOWritable() {
		super(KmerCOWritable.class);
	}

	public ArrayKmerCOWritable(KmerCOWritable[] values) {
		super(KmerCOWritable.class, values);
	}


	@Override
	public String toString() {
		String res= "ArrayKmerCOWritable: [";
		
		for(int i=0; i<this.get().length; i++){
			if(i!=0)
				res+=", ";
			res+=this.get()[i].toString();
		}

		res+= "]";

		return res;
	}

}
