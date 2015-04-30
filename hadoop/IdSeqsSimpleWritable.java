package hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * 
 * @version 1.0
 * 
 * Date: January, 18 2015
 */
public class IdSeqsSimpleWritable implements Writable, WritableComparable<IdSeqsSimpleWritable>{
	
	private Text idSeq1;
	private Text idSeq2;

	
	public IdSeqsSimpleWritable() {
		this(new Text(""), new Text(""));
	}

	public IdSeqsSimpleWritable(Text idSeq1, Text idSeq2) {
		super();
		this.idSeq1 = idSeq1;
		this.idSeq2 = idSeq2;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((idSeq1 == null) ? 0 : idSeq1.hashCode());
		result = prime * result + ((idSeq2 == null) ? 0 : idSeq2.hashCode());
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
		IdSeqsSimpleWritable other = (IdSeqsSimpleWritable) obj;
		if (idSeq1 == null) {
			if (other.idSeq1 != null)
				return false;
		} else if (!idSeq1.equals(other.idSeq1))
			return false;
		if (idSeq2 == null) {
			if (other.idSeq2 != null)
				return false;
		} else if (!idSeq2.equals(other.idSeq2))
			return false;
		return true;
	}

	public Text getIdSeq1() {
		return idSeq1;
	}

	public void setIdSeq1(Text idSeq1) {
		this.idSeq1 = idSeq1;
	}

	public Text getIdSeq2() {
		return idSeq2;
	}

	public void setIdSeq2(Text idSeq2) {
		this.idSeq2 = idSeq2;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		 idSeq1.readFields(in);
		 idSeq2.readFields(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		idSeq1.write(out);
		idSeq2.write(out);
	}
	
	@Override
	public String toString() {
		//return "[idSeq1=" + idSeq1 + ", idSeq2=" + idSeq2 + "]";
		return idSeq1 + " " + idSeq2;
	}

	@Override
	public int compareTo(IdSeqsSimpleWritable o) {
		
		String thisObject = this.getIdSeq1().toString()+" "+this.getIdSeq2().toString();
		String otherObject = o.getIdSeq1().toString()+" "+o.getIdSeq2().toString();
		
		return thisObject.compareTo(otherObject);
	}
}
