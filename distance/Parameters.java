package distance;

import java.util.Map;

public class Parameters {//TODO
	
	//int c1, int length1, int c2, int length2, int k
	private int c1;
	private int c2;
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

	public int getC1() {
		return c1;
	}
	public void setC1(int c1) {
		this.c1 = c1;
	}
	public int getC2() {
		return c2;
	}
	public void setC2(int c2) {
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
	@Override
	public String toString() {
		return "Parameters [c1=" + c1 + ", c2=" + c2 + ", length1=" + length1
				+ ", length2=" + length2 + ", k=" + k + ", kmer=" + kmer
				+ ", mapS1=" + mapS1 + ", mapS2=" + mapS2 + "]";
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + c1;
		result = prime * result + c2;
		result = prime * result + k;
		result = prime * result + ((kmer == null) ? 0 : kmer.hashCode());
		result = prime * result + length1;
		result = prime * result + length2;
		result = prime * result + ((mapS1 == null) ? 0 : mapS1.hashCode());
		result = prime * result + ((mapS2 == null) ? 0 : mapS2.hashCode());
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
		if (c1 != other.c1)
			return false;
		if (c2 != other.c2)
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
		return true;
	}
	public Parameters() {
		super();
		// TODO Auto-generated constructor stub
	}
	public Parameters(int c1, int c2, int length1, int length2, int k,
			String kmer, Map<String, Integer> mapS1, Map<String, Integer> mapS2) {
		super();
		this.c1 = c1;
		this.c2 = c2;
		this.length1 = length1;
		this.length2 = length2;
		this.k = k;
		this.kmer = kmer;
		this.mapS1 = mapS1;
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
}