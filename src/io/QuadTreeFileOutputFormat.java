package io;
/*
 * MapReduce Record Writer for Quadtree index files
 * Author: Donghan Miao
 */
import java.io.DataOutputStream;
import java.io.IOException;

import mapReduceTasks.QuadTreeWritable;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

public class QuadTreeFileOutputFormat extends FileOutputFormat<IntWritable, QuadTreeWritable> {

	protected static class QuadTreeWriter implements RecordWriter<IntWritable, QuadTreeWritable> {
		//private static final String utf8 = "UTF-8";

		private DataOutputStream out;

		public QuadTreeWriter(DataOutputStream out) throws IOException {
			this.out = out;
		}

		/**
		 * Write the object to the byte stream, handling Text as a special case.
		 * 
		 * @param o
		 *            the object to print
		 * @throws IOException
		 *             if the write throws, we pass it on
		 */
		private void writeObject(QuadTreeWritable value) throws IOException {
			value.write(out);
		}

		private void writeKey(IntWritable key) throws IOException {
			key.write(out);
		}

		public synchronized void write(IntWritable key, QuadTreeWritable value) throws IOException {

			boolean nullKey = key == null;
			boolean nullValue = value == null;

			if (nullKey && nullValue) {
				return;
			}

			IntWritable keyObj = key;

			if (!nullKey) {
				writeKey(keyObj);
			}

			if (!nullValue) {
				writeObject(value);
			}
		}

		public synchronized void close(Reporter reporter) throws IOException {
			out.close();
		}

	}

	public RecordWriter<IntWritable, QuadTreeWritable> getRecordWriter(FileSystem ignored, JobConf job,
			String name, Progressable progress) throws IOException {
		Path file = FileOutputFormat.getTaskOutputPath(job, name);
		FileSystem fs = file.getFileSystem(job);
		FSDataOutputStream fileOut = fs.create(file, progress);
		return new QuadTreeWriter(fileOut);
	}
}