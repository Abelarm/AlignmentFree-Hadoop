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
public class ArrayKmerCountWritable extends ArrayWritable{

	public ArrayKmerCountWritable() {
		super(KmerCountWritable.class);
	}

	public ArrayKmerCountWritable(KmerCountWritable[] values) {
		super(KmerCountWritable.class, values);
	}


	@Override
	public String toString() {
		String res= "ArrayKmerCountWritable: [";

		for(int i=0; i<this.get().length; i++){
			if(i!=0)
				res+=", ";
			res+=this.get()[i].toString();
		}

		res+= "]";

		return res;
	}

}
