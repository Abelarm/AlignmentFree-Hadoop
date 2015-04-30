package CV.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Store information about id genome sequence and its 4 counts related to a kmer (kword) and its substrings, according to the Composition Vector method.
 * 
 * @author Luigi Giugliano - Steven Rosario Sirchia
 * 
 * @version 1.2
 * 
 * Date: February, 16 2015
 */
public class Kmer4CountsWritable implements Writable{
	
	private Text idSeq;
	private DoubleWritable count0;
	private DoubleWritable count1;
	private DoubleWritable count1b;
	private DoubleWritable count2;

	public Kmer4CountsWritable(){
		super();
		this.idSeq = new Text("");
		this.count0 = new DoubleWritable(0);
		this.count1 = new DoubleWritable(0);
		this.count1b = new DoubleWritable(0);
		this.count2 = new DoubleWritable(0);
	}
	
	public Kmer4CountsWritable(String idS, double c0, double c1, double c1b, double c2) {
		super();
		this.idSeq = new Text(idS);
		this.count0 = new DoubleWritable(c0);
		this.count1 = new DoubleWritable(c1);
		this.count1b = new DoubleWritable(c1b);
		this.count2 = new DoubleWritable(c2);
	}

	public Kmer4CountsWritable(Text idSeq, DoubleWritable count0, DoubleWritable count1, DoubleWritable count1b, DoubleWritable count2) {
		super();
		this.idSeq = idSeq;
		this.count0 = count0;
		this.count1 = count1;
		this.count1b = count1b;
		this.count2 = count2;
	}
	
	
	public void incrementCount() {
		double newValue = this.count0.get() + 1;
		this.count0.set(newValue);
	}
	
	
	
	public Text getIdSeq() {
		return idSeq;
	}

	public void setIdSeq(Text idSeq) {
		this.idSeq = idSeq;
	}

	public DoubleWritable getCount0() {
		return count0;
	}

	public void setCount0(DoubleWritable count0) {
		this.count0 = count0;
	}

	public DoubleWritable getCount1() {
		return count1;
	}

	public void setCount1(DoubleWritable count1) {
		this.count1 = count1;
	}

	public DoubleWritable getCount1b() {
		return count1b;
	}

	public void setCount1b(DoubleWritable count1b) {
		this.count1b = count1b;
	}

	public DoubleWritable getCount2() {
		return count2;
	}

	public void setCount2(DoubleWritable count2) {
		this.count2 = count2;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((count0 == null) ? 0 : count0.hashCode());
		result = prime * result + ((count1 == null) ? 0 : count1.hashCode());
		result = prime * result + ((count1b == null) ? 0 : count1b.hashCode());
		result = prime * result + ((count2 == null) ? 0 : count2.hashCode());
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
		Kmer4CountsWritable other = (Kmer4CountsWritable) obj;
		if (count0 == null) {
			if (other.count0 != null)
				return false;
		} else if (!count0.equals(other.count0))
			return false;
		if (count1 == null) {
			if (other.count1 != null)
				return false;
		} else if (!count1.equals(other.count1))
			return false;
		if (count1b == null) {
			if (other.count1b != null)
				return false;
		} else if (!count1b.equals(other.count1b))
			return false;
		if (count2 == null) {
			if (other.count2 != null)
				return false;
		} else if (!count2.equals(other.count2))
			return false;
		if (idSeq == null) {
			if (other.idSeq != null)
				return false;
		} else if (!idSeq.equals(other.idSeq))
			return false;
		return true;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		 idSeq.readFields(in);
         count0.readFields(in);
         count1.readFields(in);
         count1b.readFields(in);
         count2.readFields(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		idSeq.write(out);
        count0.write(out);
        count1.write(out);	
        count1b.write(out);	
        count2.write(out);	
	}

	@Override
	public String toString() {
		return "KmerMultiDoubleCountWritable [idSeq=" + idSeq + ", count0="
				+ count0 + ", count1=" + count1 + ", count1b=" + count1b
				+ ", count2=" + count2 + "]";
	}
	
	

}
