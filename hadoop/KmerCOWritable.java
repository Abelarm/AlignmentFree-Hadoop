package hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Store information about id genome sequence and its count related to a kmer (kword).
 * 
 * @author Francesco Gaetano - email: f.gaetano@studenti.unisa.it 
 * 
 * @version 1.0
 * 
 * Date: February, 4 2015
 */
public class KmerCOWritable implements Writable{
	
	private Text idSeq;
	private Text object;
	
	public KmerCOWritable() {
		super();
		this.idSeq = new Text();
		this.object = new Text();
	}
	
	public KmerCOWritable(Text idSeq, Text object) {
		super();
		this.idSeq = idSeq;
		this.object = object;
	}
	
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((idSeq == null) ? 0 : idSeq.hashCode());
		result = prime * result + ((object == null) ? 0 : object.hashCode());
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
		KmerCOWritable other = (KmerCOWritable) obj;
		if (idSeq == null) {
			if (other.idSeq != null)
				return false;
		} else if (!idSeq.equals(other.idSeq))
			return false;
		if (object == null) {
			if (other.object != null)
				return false;
		} else if (!object.equals(other.object))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "KmerCOWritable [idSeq=" + idSeq + ", object=" + object + "]";
	}

	public Text getIdSeq() {
		return idSeq;
	}
	
	public void setIdSeq(Text idSeq) {
		this.idSeq = idSeq;
	}
	
	public Text getObject() {
		return object;
	}
	
	public void setObject(Text object) {
		this.object = object;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		idSeq.readFields(in);
		object.readFields(in);
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		idSeq.write(out);
		object.write(out);
	}
	
}
