package hadoop.inputsplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class ValueWritable implements Writable{

	private Text currLine;
	private Text nextLine; //puo' appartenere anche ad un altro input split. E' null se la sequenza genomica e' terminata.

	
	public ValueWritable() {
		super();
	}

	public ValueWritable(Text currLine, Text nextLine) {
		super();
		this.currLine = currLine;
		this.nextLine = nextLine;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((currLine == null) ? 0 : currLine.hashCode());
		result = prime * result
				+ ((nextLine == null) ? 0 : nextLine.hashCode());
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
		ValueWritable other = (ValueWritable) obj;
		if (currLine == null) {
			if (other.currLine != null)
				return false;
		} else if (!currLine.equals(other.currLine))
			return false;
		if (nextLine == null) {
			if (other.nextLine != null)
				return false;
		} else if (!nextLine.equals(other.nextLine))
			return false;
		return true;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		currLine.readFields(in);
		nextLine.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		currLine.write(out);
		nextLine.write(out);
	}

	public Text getNextLine() {
		return nextLine;
	}

	public void setNextLine(Text nextLine) {
		this.nextLine = nextLine;
	}

	public Text getCurrLine() {
		return currLine;
	}

	public void setCurrLine(Text currLine) {
		this.currLine = currLine;
	}
	
	@Override
	public String toString() {
		return "ValueWritable [currLine=" + currLine + ", nextLine=" + nextLine
				+ "]";
	}

}
