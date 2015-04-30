package CV.distance;

import java.util.Map;

public class Parameters {
	
	//int c1, int length1, int c2, int length2, int k
	private double c1;
	private double c2;
	private int length1;
	private int length2;
	private int k;
	private int numLettere;
	private String kmer;
	private Map<String,Integer> mapS1;
	private Map<String,Integer> mapS2;
	private String contesto1;
	private String contesto2;
	private String oggetto1;
	private String oggetto2;
	private String pattern;
	private Map<String,Double> probMap1;
	private Map<String,Double> probMap2;
	
	public Parameters() {
		super();
	}

	public Parameters(double c1, double c2, int length1,
			int length2, int k, int numLettere, String kmer,
			Map<String, Integer> mapS1, Map<String, Integer> mapS2,
			String contesto1, String contesto2, String oggetto1,
			String oggetto2, String pattern, Map<String, Double> probMap1,
			Map<String, Double> probMap2) {
		super();
		this.c1 = c1;
		this.c2 = c2;
		this.length1 = length1;
		this.length2 = length2;
		this.k = k;
		this.numLettere = numLettere;
		this.kmer = kmer;
		this.mapS1 = mapS1;
		this.mapS2 = mapS2;
		this.contesto1 = contesto1;
		this.contesto2 = contesto2;
		this.oggetto1 = oggetto1;
		this.oggetto2 = oggetto2;
		this.pattern = pattern;
		this.probMap1 = probMap1;
		this.probMap2 = probMap2;
	}


	public String getPattern() {
		return pattern;
	}

	public void setPattern(String pattern) {
		this.pattern = pattern;
	}

	
	
	public String getContesto1() {
		return contesto1;
	}

	public void setContesto1(String contesto1) {
		this.contesto1 = contesto1;
	}

	public String getContesto2() {
		return contesto2;
	}

	public void setContesto2(String contesto2) {
		this.contesto2 = contesto2;
	}

	public String getOggetto1() {
		return oggetto1;
	}

	public void setOggetto1(String oggetto1) {
		this.oggetto1 = oggetto1;
	}

	public String getOggetto2() {
		return oggetto2;
	}

	public void setOggetto2(String oggetto2) {
		this.oggetto2 = oggetto2;
	}

	public double getC1() {
		return c1;
	}
	public void setC1(double c1) {
		this.c1 = c1;
	}
	public double getC2() {
		return c2;
	}
	public void setC2(double c2) {
		this.c2 = c2;

	}
	public int getLength1() {
		return length1;
	}
	public void setLength1(int length1) {
		this.length1 = length1;
	}
	public int getLength2() {
		return length2;
	}
	public void setLength2(int length2) {
		this.length2 = length2;
	}
	public int getK() {
		return k;
	}
	public void setK(int k) {
		this.k = k;
	}
	public String getKmer() {
		return kmer;
	}
	public void setKmer(String kmer) {
		this.kmer = kmer;
	}
	public Map<String, Integer> getMapS1() {
		return mapS1;
	}
	public void setMapS1(Map<String, Integer> mapS1) {
		this.mapS1 = mapS1;
	}
	public Map<String, Integer> getMapS2() {
		return mapS2;
	}
	public void setMapS2(Map<String, Integer> mapS2) {
		this.mapS2 = mapS2;
	}

	public int getNumLettere() {
		return numLettere;
	}
	
	public void setNumLettere(int lettere) {
		numLettere = lettere;
	}

	public Map<String, Double> getProbMap1() {
		return probMap1;
	}
	
	public void setProbMap1(Map<String, Double> probMap) {
		probMap1=probMap;
	}

	public Map<String, Double> getProbMap2() {
		return probMap2;
	}
	
	public void setProbMap2(Map<String, Double> probMap) {
		probMap2=probMap;
	}

	@Override
	public String toString() {
		return "Parameters [c1=" + c1 + ", c2=" + c2 + ", length1=" + length1 + ", length2=" + length2 + ", k="
				+ k + ", numLettere=" + numLettere + ", kmer=" + kmer
				+ ", mapS1=" + mapS1 + ", mapS2=" + mapS2 + ", contesto1="
				+ contesto1 + ", contesto2=" + contesto2 + ", oggetto1="
				+ oggetto1 + ", oggetto2=" + oggetto2 + ", pattern=" + pattern
				+ ", probMap1=" + probMap1 + ", probMap2=" + probMap2 + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(c1);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(c2);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result
				+ ((contesto1 == null) ? 0 : contesto1.hashCode());
		result = prime * result
				+ ((contesto2 == null) ? 0 : contesto2.hashCode());
		result = prime * result + k;
		result = prime * result + ((kmer == null) ? 0 : kmer.hashCode());
		result = prime * result + length1;
		result = prime * result + length2;
		result = prime * result + ((mapS1 == null) ? 0 : mapS1.hashCode());
		result = prime * result + ((mapS2 == null) ? 0 : mapS2.hashCode());
		result = prime * result + numLettere;
		result = prime * result
				+ ((oggetto1 == null) ? 0 : oggetto1.hashCode());
		result = prime * result
				+ ((oggetto2 == null) ? 0 : oggetto2.hashCode());
		result = prime * result + ((pattern == null) ? 0 : pattern.hashCode());
		result = prime * result
				+ ((probMap1 == null) ? 0 : probMap1.hashCode());
		result = prime * result
				+ ((probMap2 == null) ? 0 : probMap2.hashCode());
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
		Parameters other = (Parameters) obj;
		if (Double.doubleToLongBits(c1) != Double.doubleToLongBits(other.c1))
			return false;
		if (Double.doubleToLongBits(c2) != Double.doubleToLongBits(other.c2))
			return false;
		if (contesto1 == null) {
			if (other.contesto1 != null)
				return false;
		} else if (!contesto1.equals(other.contesto1))
			return false;
		if (contesto2 == null) {
			if (other.contesto2 != null)
				return false;
		} else if (!contesto2.equals(other.contesto2))
			return false;
		if (k != other.k)
			return false;
		if (kmer == null) {
			if (other.kmer != null)
				return false;
		} else if (!kmer.equals(other.kmer))
			return false;
		if (length1 != other.length1)
			return false;
		if (length2 != other.length2)
			return false;
		if (mapS1 == null) {
			if (other.mapS1 != null)
				return false;
		} else if (!mapS1.equals(other.mapS1))
			return false;
		if (mapS2 == null) {
			if (other.mapS2 != null)
				return false;
		} else if (!mapS2.equals(other.mapS2))
			return false;
		if (numLettere != other.numLettere)
			return false;
		if (oggetto1 == null) {
			if (other.oggetto1 != null)
				return false;
		} else if (!oggetto1.equals(other.oggetto1))
			return false;
		if (oggetto2 == null) {
			if (other.oggetto2 != null)
				return false;
		} else if (!oggetto2.equals(other.oggetto2))
			return false;
		if (pattern == null) {
			if (other.pattern != null)
				return false;
		} else if (!pattern.equals(other.pattern))
			return false;
		if (probMap1 == null) {
			if (other.probMap1 != null)
				return false;
		} else if (!probMap1.equals(other.probMap1))
			return false;
		if (probMap2 == null) {
			if (other.probMap2 != null)
				return false;
		} else if (!probMap2.equals(other.probMap2))
			return false;
		return true;
	}

}