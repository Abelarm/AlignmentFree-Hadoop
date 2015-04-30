package sequential;

import java.util.Map;

/**
 * Save the information about kmers frequencies of a sequence.
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * 
 * @version 2.0
 * 
 * Date: January, 17 2015
 */
public class SequenceFrequencies {

	private Map<String, Double> allFreqKmers;
	private Integer length;
	private String name;

	public SequenceFrequencies(Map<String, Double> allFreqKmers, Integer length, String name) {
		super();
		this.allFreqKmers = allFreqKmers;
		this.length = length;
		this.name = name;
	}

	public Map<String, Double> getAllFreqKmers() {
		return allFreqKmers;
	}

	public void setAllFreqKmers(Map<String, Double> allFreqKmers) {
		this.allFreqKmers = allFreqKmers;
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
		return "Info [allFreqKmers=" + allFreqKmers + ", length=" + length
				+ ", name=" + name + "]";
	}

}
