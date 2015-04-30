package hadoop.inputsplit;

import java.io.IOException;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.RecordReader;

/**
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * 
 * @version 1.2
 * 
 * Date: January, 26 2015
 * 
 * This class reads <key, value> pairs from an InputSplit. 
 * The input file is in FASTA format.
 * A FASTA record has a header line that is the key, and data lines that are the value.
 * >header...
 * data
 * ...
 * 
 * 
 * Example:
 * >Seq1
 * TAATCCCAAATGATTATATCCTTCTCCGATCGCTAGCTATACCTTCCAGGCGATGAACTTAGACGGAATCCACTTTGCTA
 * CAACGCGATGACTCAACCGCCATGGTGGTACTAGTCGCGGAAAAGAAAGAGTAAACGCCAACGGGCTAGACACACTAATC
 * CTCCGTCCCCAACAGGTATGATACCGTTGGCTTCACTTCTA
 * >Seq2
 * CTACATTCGTAATCTCTTTGTCAGTCCTCCCGTACGTTGGCAAAGGTTCACTGGAAAAATTGCCGACGCACAGGTGCCGG
 * GCCGTGAATAGGGCCAGATGAACAAGGAAATAATCACCACCGAGGTGTGACATGCCCTCTCGGGCAACCACTCTTCCTCA
 * TACCCCCTCTGGGCTAACTCGGAGCAAAGAACTTGGTAA
 * ... 
 */
//Crea ogni record con key=idSeq e value=allGenome, quindi tale classe deve essere usata solo quando le sequenze genomiche sono di piccole dimensioni.
public class FastaRecordReader extends RecordReader<Text, Text> {

	private byte[] MARKER_RECORD_BEGIN, KEY_VAL_SEPARATOR;

	private FSDataInputStream inputFile; // Questo e' il file di input a cui appartiene l'input split corrente.

	private long startByte, endByte; // Indicano l'inizio e la fine dell'input split. In particolare, endByte e' l'inizio del prossimo input split.

	private Text currentKey; // Current key usata per memorizzare l'id della sequenza/stringa genomica in esame.

	private Text currentValue; // Current value usato per memorizzare tutta la stringa genomica in esame.


	public FastaRecordReader() {
		super();
	}


	@Override
	public void close() throws IOException {//Close the record reader.
		if(inputFile!=null)
			inputFile.close();
	}


	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return currentKey;
	}


	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return currentValue;
	}


	@Override
	public float getProgress() throws IOException, InterruptedException {//The current progress of the record reader through its data.
		//Restituisce un numero tra 0 e 1 indicante la percentuale di completamento.
		float inputSplitLength = endByte - startByte; 
		float progress = getCurrentPosition() - startByte;
		return inputSplitLength > 0 ? progress / inputSplitLength : 1; 
	}


	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {// Called once at initialization.

		Configuration job = context.getConfiguration();

		currentKey = new Text();
		currentValue = new Text();

		// Questo InputSplit e' un FileInputSplit
		FileSplit split = (FileSplit) genericSplit;
		Path path = split.getPath(); // Questo e' il pathname del file a cui appartiene l'input split corrente.

		startByte = split.getStart(); // Resituisce la posizione del primo byte nel file da processare.
		endByte = startByte + split.getLength(); // endByte e' l'ultimo byte da analizzare. In particolare, endByte e' l'inizio del prossimo input split.
		inputFile = path.getFileSystem(job).open(path); // Apertura del file a cui appartiene questo input split.
		inputFile.seek(startByte); // Mi posiziono sul primo byte appartenente all'input split.

		MARKER_RECORD_BEGIN = ">".getBytes("UTF-8"); // Tutti i nuovi record iniziano con il carattere >.
		KEY_VAL_SEPARATOR  = "\n".getBytes("UTF-8"); //Una chiave ed un valore sono separati da \n.

		//N.B. MARKER_RECORD_BEGIN e KEY_VAL_SEPARATOR sono entrambi di un solo byte. Java usa UTF-16 come rappresentazione interna dei caratteri.
	}


	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {//Usato per leggere la prossima coppia chiave-valore.

		seekNextRecordBoundary();
		/*
		 *  Posiziona il cursore al prossimo confine del record, ossia si trova il prossimo carattere > (> e' anche consumato).
		 *  Quindi, si fa la seek del file al primo carattere (diverso da >) che fa parte dell'header. 
		 */

		if(getCurrentPosition() >= endByte)  
			return false; // Ho terminato il mio input split, quindi si restituisce false.

		//System.out.println("<"+key+","+value+">");

		// Si legge il prossimo record chiave-valore.
		return readKey(currentKey) && readValue(currentValue);
	}


	public long getCurrentPosition() throws IOException {
		return inputFile.getPos();
	}


	public boolean readKey(Text key) throws IOException {

		DataOutputBuffer out = new DataOutputBuffer();

		if (readUntilMatch(KEY_VAL_SEPARATOR, true, false, out)) { // Si trova la fine della key, ossia \n che e' consumato e non viene messo nel buffer di output.
			// Quindi, il buffer di output contiene solo l'header della stringa genomica (senza i caratteri > e \n).
			key.set(new String(out.getData(), 0, out.getLength()).replaceAll("\t", " "));
			//System.out.println(">"+key);
			out.close();
			return true;
		}

		//Non ci sono piu' chiavi.
		out.close();
		return false;
	}


	public boolean readValue(Text value) throws IOException {
		DataOutputBuffer out = new DataOutputBuffer();
		readUntilMatch(MARKER_RECORD_BEGIN, false, false, out); //Si legge il prossimo value (ossia fino a quando non trovo > che non e' ne' consumato, ne' messo nel buffer di output). 
		value.set(new String(out.getData(), 0, out.getLength()).replaceAll("\n", "")); //Bisogna togliere tutti gli \n.
		//System.out.println(">"+value);

		return true;
	}


	/*
	 * pattern e' la sequenza di bytes da cercare, es. il separatore key-value oppure il marcatore di inizio record.
	 * consumePattern indica se inserire nell'output il pattern trovato.
	 * emitPattern indica se usare il buffer di output per scrivere il pattern trovato.
	 * outBufOrNull e' il buffer di output (se presente).
	 */
	private boolean readUntilMatch(byte[] pattern, boolean consumePattern, boolean emitPattern, DataOutputBuffer outBufOrNull) throws IOException {
		// Questo metodo funziona solo per pattern di un solo byte.


		long pos = getCurrentPosition(); //Restituisce la posizione corrente del cursore.

		//inputFile.read() legge il prossimo byte dall'input stream; restituisce -1 se il file termina.
		for (int numMatchingBytes = 0, b = inputFile.read(); b != -1; b = inputFile.read()) {

			byte next = (byte) b; 

			if (next == pattern[numMatchingBytes] && ++numMatchingBytes == pattern.length) {// Se ho trovato il pattern

				if (!consumePattern) // Se non devo consumare il pattern trovato ...
					inputFile.seek(pos); // ... mi posiziono all'inizio del pattern trovato (ossia torno indietro).

				if (emitPattern && outBufOrNull != null)
					outBufOrNull.write(pattern); // Si scrive il pattern trovato sul buffer.

				return true; //Ho trovato il pattern.

			} else { // Se non ho trovato il pattern

				if (outBufOrNull != null) {
					outBufOrNull.write(pattern, 0, numMatchingBytes); // Si scrivono i byte trovati.
					outBufOrNull.write(next); // Si scrive il byte corrente.
				}
				// Reset:
				numMatchingBytes = 0;
				pos = getCurrentPosition();
			}
		}

		//Se ho scorso tutto il file e non ho trovato il pattern. Quindi, potrei anche "uscire fuori" dal mio input split.
		return false;
	}

	private void seekNextRecordBoundary() throws IOException { // Posiziona il cursore al prossimo confine del record, ossia si trova il prossimo carattere successivo a > (> e' anche consumato).
		readUntilMatch(MARKER_RECORD_BEGIN, true, true, null);
	}

}

