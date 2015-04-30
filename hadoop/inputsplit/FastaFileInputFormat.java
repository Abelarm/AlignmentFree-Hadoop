package hadoop.inputsplit;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


/**
 * A FileInputFormat for reading FASTA records.
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * 
 * @version 1.1
 * 
 * Date: January, 26 2015
 */
public class FastaFileInputFormat extends FileInputFormat<Text,Text>{

	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException,InterruptedException {
		return new FastaRecordReader(); //Crea ogni record con key=idSeq e value=allGenome.
	}
}