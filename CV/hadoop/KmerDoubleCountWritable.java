package CV.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Store information about id genome sequence and its count related to a kmer (kword).
 * 
 * @author Luigi Giugliano - Steven Rosario Sirchia
 * 
 * @version 1.1
 * 
 * Date: February, 4 2015
 */
public class KmerDoubleCountWritable implements Writable{
	
	private Text idSeq;
	private DoubleWritable count;
	
	public KmerDoubleCountWritable(){
		super();
		this.idSeq = new Text("");
		this.count = new DoubleWritable(0);
	}
	
	public KmerDoubleCountWritable(String idS, double c) {
		super();
		this.idSeq = new Text(idS);
		this.count = new DoubleWritable(c);
	}

	public KmerDoubleCountWritable(Text idSeq, DoubleWritable count) {
		super();
		this.idSeq = idSeq;
		this.count = count;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((count == null) ? 0 : count.hashCode());
		result = prime * result + ((idSeq == null) ? 0 : idSeq.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		
		if (obj == null)
			return false;
		
		if (getClass() != obj.getClass())
			return false;
		
		KmerDoubleCountWritable other = (KmerDoubleCountWritable) obj;
		if (count == null) {
			if (other.count != null)
				return false;
		} else if (!count.equals(other.count))
			return false;
		if (idSeq == null) {
			if (other.idSeq != null)
				return false;
		} else if (!idSeq.equals(other.idSeq))
			return false;
		
		return true;
	}
	
	public Text getIdSeq() {
		return idSeq;
	}
	
	public void setIdSeq(Text idSeq) {
		this.idSeq = idSeq;
	}
	
	public DoubleWritable getCount() {
		return count;
	}
	
	public void setCount(DoubleWritable count) {
		this.count = count;
	}
	
	public void incrementCount() {
		double newValue = this.count.get() + 1;
		this.count.set(newValue);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		 idSeq.readFields(in);
         count.readFields(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		idSeq.write(out);
        count.write(out);		
	}
	
	@Override
	public String toString() {
		//return "KmerCount [idSeq=" + idSeq + ", count=" + count + "]";
		return "(idSeq=" + idSeq + ", count=" + count + ")";
	}

}
