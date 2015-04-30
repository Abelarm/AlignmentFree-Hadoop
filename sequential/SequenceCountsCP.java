package sequential;

import java.util.HashMap;
import java.util.Map;
/**
 * Save the information about context and object of kmers for co_phylog
 * 
 * @author team 2 
 * 
 * @version 1.0
 * 
 * Date: January, 22 2015
 */
public class SequenceCountsCP {
	
	private Map<String, String> allCountKmersCP;
	private Integer lengthCP;
	private String nameCP;
	
	
	public SequenceCountsCP() {
		super();
		this.allCountKmersCP = new HashMap<String, String>();
		this.lengthCP = 0;
		this.nameCP = null;
	}

	public SequenceCountsCP(Map<String, String> allCountKmersCP, Integer length, String name) {
		super();
		this.allCountKmersCP = allCountKmersCP;
		this.lengthCP = length;
		this.nameCP = name;
	}

	public Map<String, String> getAllCountKmersCP() {
		return allCountKmersCP;
	}

	public void setAllCountKmersCP(Map<String, String> allFreqKmers) {
		this.allCountKmersCP = allFreqKmers;
	}

	public Integer getLengthCP() {
		return lengthCP;
	}

	public void setLengthCP(Integer length) {
		this.lengthCP = length;
	}

	public String getNameCP() {
		return nameCP;
	}

	public void setName(String name) {
		this.nameCP = name;
	}

	@Override
	public String toString() {
		return "Info [allCountKmers=" + allCountKmersCP + ", length=" + lengthCP
				+ ", name=" + nameCP + "]";
	}

}
