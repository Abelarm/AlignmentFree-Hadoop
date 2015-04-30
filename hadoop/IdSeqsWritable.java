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
 * @version 2.1
 * 
 * Date: January, 24 2015
 */
public class IdSeqsWritable implements Writable, WritableComparable<IdSeqsWritable>{
	
	private Text idSeq1;
	private Text idSeq2;
	private Text pattern;
	private Text distanceClass;
	
	public IdSeqsWritable() {
		this(new Text(""), new Text(""), new Text(""), new Text(""));
	}
	
	public IdSeqsWritable(Text idSeq1, Text idSeq2, Text pattern, Text distanceClass) {
		super();
		this.idSeq1 = idSeq1;
		this.idSeq2 = idSeq2;
		this.pattern = pattern;
		this.distanceClass = distanceClass;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((distanceClass == null) ? 0 : distanceClass.hashCode());
		result = prime * result + ((idSeq1 == null) ? 0 : idSeq1.hashCode());
		result = prime * result + ((idSeq2 == null) ? 0 : idSeq2.hashCode());
		result = prime * result + ((pattern == null) ? 0 : pattern.hashCode());
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
		IdSeqsWritable other = (IdSeqsWritable) obj;
		if (distanceClass == null) {
			if (other.distanceClass != null)
				return false;
		} else if (!distanceClass.equals(other.distanceClass))
			return false;
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
		if (pattern == null) {
			if (other.pattern != null)
				return false;
		} else if (!pattern.equals(other.pattern))
			return false;
		return true;
	}
	
	@Override
	public String toString() {
//		return "IdSeqsWritable [idSeq1=" + idSeq1 + ", idSeq2=" + idSeq2
//				+ ", pattern=" + pattern + ", distanceName=" + distanceName
//				+ "]";
		
		return idSeq1 + " " + idSeq2 + " " + pattern + " " + distanceClass;
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
	
	public Text getPattern() {
		return pattern;
	}
	
	public void setPattern(Text pattern) {
		this.pattern = pattern;
	}
	
	public Text getDistanceClass() {
		return distanceClass;
	}

	public void setDistanceClass(Text distanceClass) {
		this.distanceClass = distanceClass;
	}
	
	@Override
	public int compareTo(IdSeqsWritable o) {

		String thisObject = this.getIdSeq1().toString()+" "+this.getIdSeq2().toString()+" "+this.getPattern()+" "+this.getDistanceClass();
		String otherObject = o.getIdSeq1().toString()+" "+o.getIdSeq2().toString()+" "+o.getPattern()+" "+o.getDistanceClass();
		
		return thisObject.compareTo(otherObject);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		idSeq1.readFields(in);
		idSeq2.readFields(in);
		pattern.readFields(in);
		distanceClass.readFields(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		idSeq1.write(out);
		idSeq2.write(out);
		pattern.write(out);
		distanceClass.write(out);
	}
}
