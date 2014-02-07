package io;
/*
 * MapReduce Record Reader for Quadtree index files
 * Author: Donghan Miao
 */
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import quadIndex.QuadTree;

public class QuadTreeInputFormat extends FileInputFormat<IntWritable,QuadTree> {

	public RecordReader<IntWritable,QuadTree> getRecordReader(InputSplit input,
			JobConf job, Reporter reporter) throws IOException {

		reporter.setStatus(input.toString());
		return new QuadTreeRecordReader(job, (FileSplit) input);
	}
	
	@Override
	protected boolean isSplitable(FileSystem fs, Path name){
		return false;
	}
}

class QuadTreeRecordReader implements RecordReader<IntWritable,QuadTree> {

	boolean read = false;
	DataInputStream in;
	
	public QuadTreeRecordReader(JobConf job, FileSplit split)
			throws IOException {
	    
	    Path path = split.getPath();
	    FileSystem fs	= path.getFileSystem(job);
	    FSDataInputStream filein = fs.open(path);
	    in = filein;
	}

	public boolean next(IntWritable key, QuadTree qt) throws IOException {
		if(!read){
			read = true;		
			key.readFields(in);
			qt.readFields(in);
			return true;
		}
		return false;
	}

	public IntWritable createKey() {
		return new IntWritable();
	}

	public QuadTree createValue() {
		return new QuadTree();
	}

	public long getPos() throws IOException {
		return 1;
	}

	public void close() throws IOException {
		in.close();
	}

	public float getProgress() throws IOException {
		return read? 1:0;
	}

}
