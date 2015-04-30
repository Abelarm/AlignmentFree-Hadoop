package hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class DistanceCOWritable extends DistanceGenericWritable {
	
	Text object;
	IntWritable count;
	DoubleWritable dist;
	
	public DistanceCOWritable() {
		this(new Text(), new IntWritable(), new DoubleWritable());
	}
	
	public DistanceCOWritable(Text object, IntWritable count, DoubleWritable dist) {
		super();
		this.object = object;
		this.count = count;
		this.dist = dist;
	}
	
		
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((count == null) ? 0 : count.hashCode());
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
		DistanceCOWritable other = (DistanceCOWritable) obj;
		if (count == null) {
			if (other.count != null)
				return false;
		} else if (!count.equals(other.count))
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
		return "DistanceCOWritable [object=" + object + ", count=" + count
				+ "]";
	}

		
	public Text getObject() {
		return object;
	}
	
	public void setObject(Text object) {
		this.object = object;
	}
	
	public IntWritable getCount() {
		return count;
	}
	
	public void setCount(IntWritable count) {
		this.count = count;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		object.readFields(in);
		count.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		object.write(out);
		count.write(out);
		
	}

	public DoubleWritable getDist() {
		return dist;
	}
	
	public void setDist(DoubleWritable dist) {
		this.dist = dist;
	}

}
