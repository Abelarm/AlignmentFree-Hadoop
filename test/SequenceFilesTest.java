package test;

import hadoop.ArrayKmerCountWritable;
import hadoop.KmerCOWritable;
import hadoop.KmerCountWritable;
import hadoop.KmerGenericWritable;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import utility.Constant;

/**
 * Test class.
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * 
 * @version 1.1
 * 
 * Date: January, 24 2015
 */
public class SequenceFilesTest {

	public static void main(String[] args){
		try {
			readSeqFile();
			//testSeqFileReadWrite();
			//testSeqFileReadWrite4();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void testSeqFileReadWrite() throws IOException {
		Configuration conf = new Configuration();
		//FileSystem fs = FileSystem.getLocal(conf);
		Path seqFilePath = new Path("file.seq");
		SequenceFile.Writer writer = SequenceFile.createWriter(conf, Writer.file(seqFilePath), Writer.keyClass(Text.class),
				Writer.valueClass(IntWritable.class));

		writer.append(new Text("key1"), new IntWritable(1));
		writer.append(new Text("key2"), new IntWritable(2));

		writer.close();

		SequenceFile.Reader reader = new SequenceFile.Reader(conf,Reader.file(seqFilePath));

		Text key = new Text();
		IntWritable val = new IntWritable();

		while (reader.next(key, val)) {
			System.err.println(key + "\t" + val);
		}

		reader.close();
	}

	//	public static void testSeqFileReadWrite2() throws IOException {
	//		Configuration conf = new Configuration();
	//		Path seqFilePath = new Path("file.seq");
	//		
	//		System.out.println("WRITE");
	//		
	//		SequenceFile.Writer writer = SequenceFile.createWriter(conf, Writer.file(seqFilePath), Writer.keyClass(Text.class),
	//				Writer.valueClass(KmerCountWritable.class));
	//
	//		writer.append(new Text("key1"), new KmerCountWritable("A",1));
	//		writer.append(new Text("key2"), new KmerCountWritable("B",2));
	//
	//		writer.close();
	//		
	//		System.out.println("READ");
	//
	//		SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(seqFilePath));
	//
	//		Text key = new Text();
	//		KmerCountWritable val = new KmerCountWritable();
	//
	//		while (reader.next(key, val)) {
	//			System.err.println(key + "\t" + val);
	//		}
	//
	//		reader.close();
	//	}

	//	public static void testSeqFileReadWrite3() throws IOException {
	//		Configuration conf = new Configuration();
	//		Path seqFilePath = new Path("file.seq");
	//		
	//		System.out.println("WRITE");
	//		
	//		SequenceFile.Writer writer = SequenceFile.createWriter(conf, Writer.file(seqFilePath), Writer.keyClass(Text.class),
	//				Writer.valueClass(ArrayWritable.class));
	//
	//		KmerCountWritable[] t = new KmerCountWritable[2];
	//		t[0]=new KmerCountWritable("A", 1);
	//		t[1]=new KmerCountWritable("B", 2);
	//		ArrayWritable aw = new ArrayWritable(KmerCountWritable.class);
	//		aw.set(t);
	//		writer.append(new Text("key1"), aw);
	//		
	//		
	//		//t = new KmerCountWritable[2];
	//		t[0]=new KmerCountWritable("C", 1);
	//		t[1]=new KmerCountWritable("D", 2);
	//		//aw = new ArrayWritable(KmerCountWritable.class);
	//		aw.set(t);
	//		writer.append(new Text("key2"), aw);
	//
	//
	//		writer.close();
	//		
	//		System.out.println("READ");
	//
	//		SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(seqFilePath));
	//
	//		Text key = new Text();
	//		ArrayWritable val = new ArrayWritable(KmerCountWritable.class);
	//
	//		while (reader.next(key, val)) {
	//			//System.err.println(key + "\t" + val);
	//			System.out.print(key+" ");
	//			Writable[] ws = val.get();
	//			for(Writable w : ws)
	//				System.out.print(w+" ");
	//			System.out.println();
	//			
	//			//KmerCountWritable[] kcw = (KmerCountWritable) val.get();
	//			
	//		}
	//
	//		reader.close();
	//	}

	public static void testSeqFileReadWrite4() throws IOException {
		Configuration conf = new Configuration();
		Path seqFilePath = new Path("file.seq");

		System.out.println("WRITE");

		SequenceFile.Writer writer = SequenceFile.createWriter(conf, Writer.file(seqFilePath), Writer.keyClass(Text.class),
				Writer.valueClass(Writable.class));

		KmerCountWritable[] t = new KmerCountWritable[2];
		t[0]=new KmerCountWritable("A", 1);
		t[1]=new KmerCountWritable("B", 2);
		ArrayKmerCountWritable aw = new ArrayKmerCountWritable(t);
		writer.append(new Text("key1"), aw);


		t[0]=new KmerCountWritable("C", 3);
		t[1]=new KmerCountWritable("D", 4);
		aw.set(t);
		writer.append(new Text("key2"), aw);


		writer.close();

		System.out.println("READ");
		//
		SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(seqFilePath));

		Text key = new Text();
		ArrayKmerCountWritable val = new ArrayKmerCountWritable();

		while (reader.next(key, val)) {
			//System.err.println(key + "\t" + val);
			System.out.print(key+" ");
			Writable[] ws = val.get();
			for(Writable w : ws)
				System.out.print(w+" ");
			System.out.println();

			//KmerCountWritable[] kcw = (KmerCountWritable) val.get();

		}

		reader.close();
	}


	public static void readSeqFile() throws IOException {
		Configuration conf = new Configuration();
		Path seqFilePath = new Path("./data/HDFS/HAFS/OUTPUT/STEP_I/seq-r-00000");

		SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(seqFilePath));

		Text key = new Text();
		KmerGenericWritable val = new KmerGenericWritable();

		while (reader.next(key, val)) {
			//System.err.println(key + "\t" + val);
			//System.out.print(key+" ");
			Writable ws = val.get();
			
			System.out.println(ws);

		}


		reader.close();
	}

}