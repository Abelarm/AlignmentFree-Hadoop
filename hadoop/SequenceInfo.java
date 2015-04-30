package hadoop;

import org.apache.hadoop.io.Text;

/**
 * Compute k-tuple distances between DNA sequences.
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * 
 * @version 1.0
 * 
 * Date: January, 21 2015
 */
public class SequenceInfo {
	
	private Text id;
	private Integer length;
	
	public SequenceInfo() {
		super();
	}
	
	public SequenceInfo(Text id, Integer length) {
		super();
		this.id = id;
		this.length = length;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((length == null) ? 0 : length.hashCode());
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
	
		SequenceInfo other = (SequenceInfo) obj;
		
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.toString().equalsIgnoreCase(other.id.toString()))
			return false;
		if (length == null) {
			if (other.length != null)
				return false;
		} else if (!length.equals(other.length))
			return false;
		return true;
	}
	
	public Text getId() {
		return id;
	}
	
	public void setId(Text id) {
		this.id = id;
	}
	
	public Integer getLength() {
		return length;
	}
	
	public void setLength(Integer length) {
		this.length = length;
	}
	
	@Override
	public String toString() {
		return "SequenceInfo [id=" + id.toString() + ", length=" + length + "]";
	}
	

}
