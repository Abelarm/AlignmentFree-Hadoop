package CV.sequential;

import java.util.ArrayList;
import java.util.Map;

/**
 * Save the information about kmers counts of a sequence.
 * 
 * @author Luigi Giugliano - Steven Rosario Sirchia
 * 
 * @version 1.1
 * 
 * Date: February, 16 2015
 */
public class Sequence4Counts {

	private Map<String, ArrayList<Double>> allCountKmers;
	private Integer length;
	private String name;

	public Sequence4Counts(Map<String, ArrayList<Double>> allCountKmers, Integer length, String name) {
		super();
		this.allCountKmers = allCountKmers;
		this.length = length;
		this.name = name;
	}

	public Map<String, ArrayList<Double>> getAllCountKmers() {
		return allCountKmers;
	}

	public void setAllCountKmers(Map<String, ArrayList<Double>> allFreqKmers) {
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
