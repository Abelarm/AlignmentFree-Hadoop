/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hadoop.inputsplit;

import hadoop.HadoopUtil;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import utility.Constant;

/**
 * 
 * @author Apache Software Foundation (ASF) 
 * Modified by: Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * 
 * @version 1.5
 * 
 * Date: February, 1 2015
 * 
 * Code from org.apache.hadoop.mapreduce.lib.input.LineRecordReader.java
 * 
 * This class reads <key, value> pair from an InputSplit. 
 * The input file is in FASTA format.
 * This class is used when there is an unique large genomic sequence/string in a FASTA file.
 * Assumption: A file contains a single very large string genomics.
 * 
 * Example input file:
 * >Seq1
 * ACTG
 * CTGACTGA
 * GACTGACTGACT
 * TGACTGACTGACTGAC
 * ACTGACTGACTGACTGACTG
 * 
 * Records:
 * <Seq1, (ACTGCTGACTGATGAC,CTG)>
 * <Seq1, (CTGACTGACTGACTGA,GAC)>
 * <Seq1, (GACTGACTGACTTGAC,TGA)>
 * <Seq1, (TGACTGACTGACTTGA,ACT)>
 * <Seq1, (ACTG,)>
 * 
 * In <IdSeq, (currLine, kcharsFromNextLine)>, kcharsFromNextLine is almost the size of the length, e.g. 80.
 * 
 */
/*
 * In questo caso ogni file di input è molto grande e contiene una sola sequenza genomica.
 * Tutte le righe del file sono lunghe Constant.FASTA_MAX_LINE_LENGTH=80 caratteri tranne la prima (id delle sequenza).
 * L'ultima riga può contenere meno di Constant.FASTA_MAX_LINE_LENGTH.
 * Nel nostro caso la dimensione del pattern k deve essere minore o uguale a Constant.FASTA_MAX_LINE_LENGTH.
 */
public class FastaLineRecordReader extends RecordReader<Text, ValueWritable> {

	private static final Log LOG = LogFactory.getLog(FastaLineRecordReader.class);

	private CompressionCodecFactory compressionCodecs = null;
	private long start;
	private long pos;
	private long end;
	private LineReader in;
	private int maxLineLength;
	private Seekable filePosition;
	private CompressionCodec codec;
	private Decompressor decompressor;
	private byte[] recordDelimiterBytes = null;
	private Path file;
	private boolean done;
	private Text currentKey = null;
	private Text value = null;
	private ValueWritable currentValue = null;
	private Text tmpValue = null;
	private Text oldValue;
	private Text tmp;
	private int maxK = 0;
	private int numErrors = 0;


	public FastaLineRecordReader() {
		try {
			this.recordDelimiterBytes = "\n".getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			LOG.error(e.getMessage());
		}
	}

	public FastaLineRecordReader(byte[] recordDelimiter) {
		this.recordDelimiterBytes = recordDelimiter;
	}

	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {

		FileSplit split = (FileSplit) genericSplit;
		Configuration job = context.getConfiguration();

		done = false;

		this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
		start = split.getStart();
		end = start + split.getLength();

		file = split.getPath();
		compressionCodecs = new CompressionCodecFactory(job);
		codec = compressionCodecs.getCodec(file);

		currentValue = new ValueWritable();
		value = new Text();
		tmpValue = new Text();
		tmp = new Text();

		// open the file and seek to the start of the split
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());

		String homeHdfs = context.getConfiguration().get("HDFS_HOME_DIR");
		//maxK = HadoopUtil.getMaxkFromPatterns(fs, new Path(homeHdfs+Constant.HDFS_PATTERNS_FILE_HDFS));


		if (isCompressedInput()) {
			decompressor = CodecPool.getDecompressor(codec);
			if (codec instanceof SplittableCompressionCodec) {
				final SplitCompressionInputStream cIn =
						((SplittableCompressionCodec)codec).createInputStream(
								fileIn, decompressor, start, end,
								SplittableCompressionCodec.READ_MODE.BYBLOCK);
				in = new LineReader(cIn, job, recordDelimiterBytes);
				start = cIn.getAdjustedStart();
				end = cIn.getAdjustedEnd();
				filePosition = cIn;
			} else {
				in = new LineReader(codec.createInputStream(fileIn, decompressor), job,
						recordDelimiterBytes);
				filePosition = fileIn;
			}
		} else {
			fileIn.seek(start);
			in = new LineReader(fileIn, job, recordDelimiterBytes);
			filePosition = fileIn;
		}
		// If this is not the first split, we always throw away first record
		// because we always (except the last split) read one extra line in
		// next() method.
		if (start != 0) {
			start += in.readLine(new Text(), 0, maxBytesToConsume(start));
		}
		this.pos = start;

		setKeySeq(fs, job); //Set currentKey

		nextMyKeyValue(); //Leggo il primo record se esiste.


	}

	private void setKeySeq(FileSystem fs, Configuration job){ //Set currentKey

		if(Constant.SPLIT2_DEBUG_MODE)
			currentKey = new Text(file.getName()+"."+start);
		else{
			try{
				LineReader reader = new LineReader(fs.open(file), job, recordDelimiterBytes);
				currentKey = new Text();
				reader.readLine(currentKey, maxLineLength);
				reader.close();
				currentKey.set(currentKey.toString().replaceAll(">", ""));
			}
			catch(Exception e){
				LOG.error(e.getMessage());
				currentKey = new Text(file.getName());
			}
		}

	}

	private boolean isCompressedInput() {
		return (codec != null);
	}

	private int maxBytesToConsume(long pos) {
		return isCompressedInput()
				? Integer.MAX_VALUE
						: (int) Math.max(Math.min(Integer.MAX_VALUE, end - pos), maxLineLength);
	}

	private long getFilePosition() throws IOException {
		long retVal;
		if (isCompressedInput() && null != filePosition) {
			retVal = filePosition.getPos();
		} else {
			retVal = pos;
		}
		return retVal;
	}

	private int skipUtfByteOrderMark() throws IOException {
		// Strip BOM(Byte Order Mark)
		// Text only support UTF-8, we only need to check UTF-8 BOM
		// (0xEF,0xBB,0xBF) at the start of the text stream.
		int newMaxLineLength = (int) Math.min(3L + (long) maxLineLength,
				Integer.MAX_VALUE);
		int newSize = in.readLine(value, newMaxLineLength, maxBytesToConsume(pos));
		// Even we read 3 extra bytes for the first line,
		// we won't alter existing behavior (no backwards incompat issue).
		// Because the newSize is less than maxLineLength and
		// the number of bytes copied to Text is always no more than newSize.
		// If the return size from readLine is not less than maxLineLength,
		// we will discard the current line and read the next line.
		pos += newSize;
		int textLength = value.getLength();
		byte[] textBytes = value.getBytes();
		if ((textLength >= 3) && (textBytes[0] == (byte)0xEF) &&
				(textBytes[1] == (byte)0xBB) && (textBytes[2] == (byte)0xBF)) {
			// find UTF-8 BOM, strip it.
			LOG.info("Found UTF-8 BOM and skipped it");
			textLength -= 3;
			newSize -= 3;
			if (textLength > 0) {
				// It may work to use the same buffer and not do the copyBytes
				textBytes = value.copyBytes();
				value.set(textBytes, 3, textLength);
			} else {
				value.clear();
			}
		}
		return newSize;
	}


	public boolean nextMyKeyValue() throws IOException {

		if(value==null)
			value = new Text();

		int newSize = 0;
		// We always read one extra line, which lies outside the upper split limit i.e. (end - 1)
		while (getFilePosition() <= end) {
			if (pos == 0) {
				newSize = skipUtfByteOrderMark();
			} else {
				newSize = in.readLine(value, maxLineLength, maxBytesToConsume(pos));
				/*
				 *     maxLineLength - the maximum number of bytes to store into str; the rest of the line is silently discarded.
				 *     
				 *     maxBytesToConsume - the maximum number of bytes to consume in this call. 
				 *     This is only a hint, because if the line cross this threshold, we allow it to happen. 
				 *     It can overshoot potentially by as much as one buffer length. 
				 */

				pos += newSize;
			}

			if ((newSize == 0) || (newSize < maxLineLength)){
				if(!value.toString().startsWith(">")) 
					break;
			}

			// line too long. try again
			LOG.info("Skipped line of size " + newSize + " at pos " + 
					(pos - newSize));
		}

		if (newSize == 0) {
			value = null;
			return false;
		} else {
			//System.err.println(currentKey+" "+currentValue);
			return true;
		}
	}


	public boolean nextOtherKeyValue() throws IOException {

		if(value==null)
			value = new Text();

		int newSize = 0;
		// We always read one extra line, which lies outside the upper split limit i.e. (end - 1)
		while (true) {
			if (pos == 0) {
				newSize = skipUtfByteOrderMark();
			} else {
				newSize = in.readLine(value, maxLineLength, maxBytesToConsume(pos));
				pos += newSize;
			}

			if ((newSize == 0) || (newSize < maxLineLength)) {
				break;
			}

			// line too long. try again
			LOG.info("Skipped line of size " + newSize + " at pos " + 
					(pos - newSize));
		}
		if (newSize == 0) {
			if(Constant.DEBUG_MODE)
				System.out.println("END INPUT FILE");
			//END INPUT FILE
			value = null;
			return false;
		} else {
			if(Constant.DEBUG_MODE)
				System.out.println("LINE AT SPLIT+1");
			//LINE AT SPLIT+1
			return true;
		}
	}


	public boolean nextKeyValue() throws IOException {

		if(value == null || done)
			return false;

		/* Invece di "tmpValue = new Text(value.toString());" 
		 * uso le tre istruzioni sotto per riciclare gli oggetti.
		 * 
		 */
		oldValue = tmpValue;
		tmpValue = value; 
		value = oldValue;

		boolean res = nextMyKeyValue();

		if(!res){
			nextOtherKeyValue();
			done = true;
		}

		//System.err.println(currentKey+" "+tmpValue+ "-"+value);
		currentValue.setCurrLine(tmpValue);

		int c = 0;

		if(Constant.SPLIT2_DEBUG_MODE)
			currentValue.setNextLine(value);
		else{//Esecuzione normale.
			try{
				if(value!=null && value.getLength()>0){
					/* Scrivo solo la stringa che mi interessa. */
					c = Math.min(value.toString().length(), maxK); //Si prendono i primi k caratteri se esistono.
					tmp.set(value.toString().substring(0, c));
					currentValue.setNextLine(tmp);
				}
				else
					currentValue.setNextLine(null);
			}
			catch(Exception e){
				e.printStackTrace();
				numErrors++;
			}
		}

		return true;

	}


	@Override
	public Text getCurrentKey() {
		return currentKey;
	}

	@Override
	public ValueWritable getCurrentValue() {
		return currentValue;
	}

	/**
	 * Get the progress within the split
	 */
	public float getProgress() {

		if(done)
			return 1.0f;

		if (start == end) {
			return 0.0f;
		} else {
			try { 
				return Math.min(1.0f, (getFilePosition() - start)
						/ (float) (end - start));
			} catch (IOException ioe) {
				throw new RuntimeException(ioe);
			}
		}
	}

	public synchronized void close() throws IOException {
		try {
			if (in != null) {
				in.close();
			}
		} finally {
			if (decompressor != null) {
				CodecPool.returnDecompressor(decompressor);
			}
			if(Constant.DEBUG_MODE)
				System.out.println("Number of errors: "+numErrors);
		}
	}
}
