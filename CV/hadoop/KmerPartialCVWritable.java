package CV.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Store information about a kmer and its partial distance, according to the Composition Vector method.
 * 
 * @author Luigi Giugliano - Steven Rosario Sirchia
 * 
 * @version 1.2
 * 
 * Date: February, 16 2015
 */
public class KmerPartialCVWritable implements Writable{

	private Text kmer;
	private DoubleWritable top;
	private DoubleWritable down1;
	private DoubleWritable down2;
	
	public KmerPartialCVWritable(){
		super();
		this.kmer = new Text("");
		this.top = new DoubleWritable(0);
		this.down1 = new DoubleWritable(0);
		this.down2 = new DoubleWritable(0);
	}
	
	public KmerPartialCVWritable(String kmer, double t, double d1, double d2) {
		super();
		this.kmer = new Text(kmer);
		this.top = new DoubleWritable(t);
		this.down1 = new DoubleWritable(d1);
		this.down2 = new DoubleWritable(d2);
	}

	public KmerPartialCVWritable(Text kmer, DoubleWritable t, DoubleWritable d1, DoubleWritable d2) {
		super();
		this.kmer = new Text(kmer);
		this.top = t;
		this.down1 = d1;
		this.down2 = d2;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		kmer.readFields(in);
        top.readFields(in);
        down1.readFields(in);
        down2.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		kmer.write(out);
        top.write(out);
        down1.write(out);
        down2.write(out);
	}

	public Text getKmer() {
		return kmer;
	}

	public void setKmer(Text kmer) {
		this.kmer = kmer;
	}

	public DoubleWritable getTop() {
		return top;
	}

	public void setTop(DoubleWritable top) {
		this.top = top;
	}

	public DoubleWritable getDown1() {
		return down1;
	}

	public void setDown1(DoubleWritable down1) {
		this.down1 = down1;
	}

	public DoubleWritable getDown2() {
		return down2;
	}

	public void setDown2(DoubleWritable down2) {
		this.down2 = down2;
	}

	@Override
	public String toString() {
		return "KmerPartialCVWritable [kmer=" + kmer + ", top=" + top
				+ ", down1=" + down1 + ", down2=" + down2 + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((down1 == null) ? 0 : down1.hashCode());
		result = prime * result + ((down2 == null) ? 0 : down2.hashCode());
		result = prime * result + ((kmer == null) ? 0 : kmer.hashCode());
		result = prime * result + ((top == null) ? 0 : top.hashCode());
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
		KmerPartialCVWritable other = (KmerPartialCVWritable) obj;
		if (down1 == null) {
			if (other.down1 != null)
				return false;
		} else if (!down1.equals(other.down1))
			return false;
		if (down2 == null) {
			if (other.down2 != null)
				return false;
		} else if (!down2.equals(other.down2))
			return false;
		if (kmer == null) {
			if (other.kmer != null)
				return false;
		} else if (!kmer.equals(other.kmer))
			return false;
		if (top == null) {
			if (other.top != null)
				return false;
		} else if (!top.equals(other.top))
			return false;
		return true;
	}
	
}
