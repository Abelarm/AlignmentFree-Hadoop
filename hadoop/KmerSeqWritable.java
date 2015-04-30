package hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Store information about a kword and an id genome sequence.
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * 
 * @version 1.0
 * 
 * Date: January, 18 2015
 */
public class KmerSeqWritable implements Writable{
	
	private Text kmer;
	private Text idSeq;
	
	public KmerSeqWritable() {
		super();
		kmer = new Text("");
		idSeq = new Text("");
	}
	
	public KmerSeqWritable(Text kmer, Text idSeq) {
		super();
		this.kmer = kmer;
		this.idSeq = idSeq;
	}
	
	public Text getKmer() {
		return kmer;
	}
	
	public void setKmer(Text kmer) {
		this.kmer = kmer;
	}
	
	public Text getIdSeq() {
		return idSeq;
	}
	
	public void setIdSeq(Text idSeq) {
		this.idSeq = idSeq;
	}
	
	@Override
	public String toString() {
		return "KmerSeqWritable [kmer=" + kmer + ", idSeq=" + idSeq + "]";
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((idSeq == null) ? 0 : idSeq.hashCode());
		result = prime * result + ((kmer == null) ? 0 : kmer.hashCode());
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
		KmerSeqWritable other = (KmerSeqWritable) obj;
		if (idSeq == null) {
			if (other.idSeq != null)
				return false;
		} else if (!idSeq.equals(other.idSeq))
			return false;
		if (kmer == null) {
			if (other.kmer != null)
				return false;
		} else if (!kmer.equals(other.kmer))
			return false;
		return true;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		kmer.readFields(in);
		idSeq.readFields(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		kmer.write(out);
		idSeq.write(out);
	}
	
}
