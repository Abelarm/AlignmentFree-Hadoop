package hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * 
 * @version 1.2
 * 
 * Date: January, 18 2015
 */
public class KmerPartialDistanceWritable implements Writable{
	
	private Text kword;
	private DoubleWritable val;
	
	public KmerPartialDistanceWritable() {
		this(new Text(""), new DoubleWritable(0));
	}
	
	public KmerPartialDistanceWritable(Text kword, DoubleWritable val) {
		super();
		this.kword = kword;
		this.val = val;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((val == null) ? 0 : val.hashCode());
		result = prime * result + ((kword == null) ? 0 : kword.hashCode());
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
		KmerPartialDistanceWritable other = (KmerPartialDistanceWritable) obj;
		if (val == null) {
			if (other.val != null)
				return false;
		} else if (!val.equals(other.val))
			return false;
		if (kword == null) {
			if (other.kword != null)
				return false;
		} else if (!kword.equals(other.kword))
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		return "[kword=" + kword + ", val=" + val + "]";
	}
	
	public Text getKword() {
		return kword;
	}
	
	public void setKword(Text kword) {
		this.kword = kword;
	}
	
	public DoubleWritable getVal() {
		return val;
	}
	
	public void setVal(DoubleWritable val) {
		this.val = val;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		 kword.readFields(in);
         val.readFields(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		kword.write(out);
        val.write(out);	
	}
}
