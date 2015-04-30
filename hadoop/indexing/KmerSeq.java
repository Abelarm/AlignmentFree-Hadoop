package hadoop.indexing;

/**
 * Store information about a kword and an id genome sequence.
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * 
 * @version 1.0
 * 
 * Date: January, 18 2015
 */
public class KmerSeq{

	private String kmer;
	private String idSeq;

	public KmerSeq() {
		super();
	}

	public KmerSeq(String kmer, String idSeq) {
		super();
		this.kmer = kmer;
		this.idSeq = idSeq;
	}

	public String getKmer() {
		return kmer;
	}

	public void setKmer(String kmer) {
		this.kmer = kmer;
	}

	public String getIdSeq() {
		return idSeq;
	}

	public void setIdSeq(String idSeq) {
		this.idSeq = idSeq;
	}

	@Override
	public String toString() {
		//return "KmerSeqWritable [kmer=" + kmer + ", idSeq=" + idSeq + "]";
		return "(kmer=" + kmer + ", idSeq=" + idSeq + ")";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((idSeq == null) ? 0 : idSeq.hashCode());
		result = prime * result + ((kmer == null) ? 0 : kmer.hashCode());
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
		KmerSeq other = (KmerSeq) obj;
		if (idSeq == null) {
			if (other.idSeq != null)
				return false;
		} else if (!idSeq.equals(other.idSeq))
			return false;
		if (kmer == null) {
			if (other.kmer != null)
				return false;
		} else if (!kmer.equals(other.kmer))
			return false;
		return true;
	}

}
