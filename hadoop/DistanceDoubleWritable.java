package hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class DistanceDoubleWritable extends DistanceGenericWritable {
	
	DoubleWritable dist;
	IntWritable count;
	
	
	public DistanceDoubleWritable() {
		this(new DoubleWritable(), new IntWritable());
		
			}
	
	public DistanceDoubleWritable(DoubleWritable dist, IntWritable count) {
		super();
		this.dist = dist;
		this.count = count;
	}

	
	
	

	public DoubleWritable getDist() {
		return dist;
	}

	public void setDist(DoubleWritable dist) {
		this.dist = dist;
	}

	public IntWritable getCount() {
		return count;
	}

	public void setCount(IntWritable count) {
		this.count = count;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		dist.readFields(in);
		count.readFields(in);

	}

	@Override
	public void write(DataOutput out) throws IOException {
		dist.write(out);
		count.write(out);
	
	}

	@Override
	public String toString() {
		return "DistanceDoubleWritable [dist=" + dist + ", count=" + count + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((count == null) ? 0 : count.hashCode());
		result = prime * result + ((dist == null) ? 0 : dist.hashCode());
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
		DistanceDoubleWritable other = (DistanceDoubleWritable) obj;
		if (count == null) {
			if (other.count != null)
				return false;
		} else if (!count.equals(other.count))
			return false;
		if (dist == null) {
			if (other.dist != null)
				return false;
		} else if (!dist.equals(other.dist))
			return false;
		return true;
	}

	
	
	

}
