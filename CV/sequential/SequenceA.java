package CV.sequential;

import java.util.Map;

/**
 * Save the information about kmers A of a sequence, calculated with the Composition Vector method.
 * 
 * @author Luigi Giugliano - Steven Rosario Sirchia
 * 
 * @version 1.1
 * 
 * Date: February, 16 2015
 */
public class SequenceA {

	private Map<String, Double> allA;
	private Integer length;
	private String name;

	public SequenceA(Map<String, Double> allA, Integer length, String name) {
		super();
		this.allA = allA;
		this.length = length;
		this.name = name;
	}

	public Map<String, Double> getAllA() {
		return allA;
	}

	public void setAllA(Map<String, Double> allFreqKmers) {
		this.allA = allFreqKmers;
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
		return "Info [allCountKmers=" + allA + ", length=" + length
				+ ", name=" + name + "]";
	}

}
