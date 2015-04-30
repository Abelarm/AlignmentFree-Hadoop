package sequential;

import java.util.Map;

/**
 * Save the information about kmers counts of a sequence.
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * 
 * @version 1.0
 * 
 * Date: January, 22 2015
 */
public class SequenceCounts {

	private Map<String, Integer> allCountKmers;
	private Integer length;
	private String name;

	public SequenceCounts(Map<String, Integer> allCountKmers, Integer length, String name) {
		super();
		this.allCountKmers = allCountKmers;
		this.length = length;
		this.name = name;
	}

	public Map<String, Integer> getAllCountKmers() {
		return allCountKmers;
	}

	public void setAllCountKmers(Map<String, Integer> allFreqKmers) {
		this.allCountKmers = allFreqKmers;
	}

	public Integer getLength() {
		return length;
	}

	public void setLength(Integer length) {
		this.length = length;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public String toString() {
		return "Info [allCountKmers=" + allCountKmers + ", length=" + length
				+ ", name=" + name + "]";
	}

}
