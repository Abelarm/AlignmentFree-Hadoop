package CV.hadoop;

import org.apache.hadoop.io.ArrayWritable;

/**Store multiple objects of the type Kmer4CountsWritable
 * 
 * @author Luigi Giugliano - Steven Rosario Sirchia
 * 
 * @version 1.1
 * 
 * Date: February, 16 2015
 */
public class ArrayKmer4CountsWritable extends ArrayWritable{

	public ArrayKmer4CountsWritable() {
		super(Kmer4CountsWritable.class);
	}

	public ArrayKmer4CountsWritable(KmerDoubleCountWritable[] values) {
		super(Kmer4CountsWritable.class, values);
	}


	@Override
	public String toString() {
		String res= "ArrayMultiDoubleKmerCountWritable: [";

		for(int i=0; i<this.get().length; i++){
			if(i!=0)
				res+=", ";
			res+=this.get()[i].toString();
		}

		res+= "]";

		return res;
	}

}
