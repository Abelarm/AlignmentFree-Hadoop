package hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * 
 * @version 1.0
 * 
 * Date: January, 18 2015
 */
//Usato nel secondo job per memorizzare la kword e due contatori di due sequenze (ordinati come la chiave).
public class KmerCountsWritableBis implements Writable{
	
	private Text kword;
	private IntWritable c1, c2;
	
	public KmerCountsWritableBis() {
		this(new Text(""), new IntWritable(0), new IntWritable(0));
	}
	
	public KmerCountsWritableBis(Text kword, IntWritable c1, IntWritable c2) {
		super();
		this.kword = kword;
		this.c1 = c1;
		this.c2 = c2;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((c1 == null) ? 0 : c1.hashCode());
		result = prime * result + ((c2 == null) ? 0 : c2.hashCode());
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
		KmerCountsWritableBis other = (KmerCountsWritableBis) obj;
		if (c1 == null) {
			if (other.c1 != null)
				return false;
		} else if (!c1.equals(other.c1))
			return false;
		if (c2 == null) {
			if (other.c2 != null)
				return false;
		} else if (!c2.equals(other.c2))
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
		return "[kword=" + kword + ", c1=" + c1 + ", c2="
				+ c2 + "]";
	}
	
	public Text getKword() {
		return kword;
	}
	
	public void setKword(Text kword) {
		this.kword = kword;
	}
	
	public IntWritable getC1() {
		return c1;
	}
	
	public void setC1(IntWritable c1) {
		this.c1 = c1;
	}
	
	public IntWritable getC2() {
		return c2;
	}
	
	public void setC2(IntWritable c2) {
		this.c2 = c2;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		 kword.readFields(in);
         c1.readFields(in);
         c2.readFields(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		kword.write(out);
        c1.write(out);	
        c2.write(out);	
	}
}
