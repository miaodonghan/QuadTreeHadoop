package mapReduceTasks;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import quadIndex.InputParser;
import quadIndex.QuadTree;
import quadIndex.SpatialObj;

/*
 * This MapReduce Job is to creating quadtree index
 * on Hadoop.
 * Author: Donghan Miao
 * 
 * */
class Map extends MapReduceBase implements
		Mapper<LongWritable, Text, IntWritable, Text> {

	static final int NUM_OF_NODES = 1;

	@Override
	public void map(LongWritable key, Text value, // input key, value
			OutputCollector<IntWritable, Text> output, Reporter reporter)
			throws IOException {

		output.collect(
				new IntWritable((int) (key.get() % (long) NUM_OF_NODES)), value);
	}

}

class Reduce extends MapReduceBase implements
		Reducer<IntWritable, Text, IntWritable, QuadTree> {

	@Override
	public void reduce(IntWritable key, Iterator<Text> values,
			OutputCollector<IntWritable, QuadTree> output, Reporter reporter)
			throws IOException {
		Configuration config = new Configuration();
		config.set("fs.default.name", "hdfs://127.0.0.1:9000/");

		FileSystem dfs = FileSystem.get(config);
		//dfs.mkdirs(new Path(dfs.getWorkingDirectory()+ "/raw"));
		FSDataOutputStream out = dfs.create(new Path(dfs.getWorkingDirectory()
				+ "/out/" + key.toString()+".rawdata"));
		
		long offset = 0L;
		QuadTree quad = new QuadTree();

		while (values.hasNext()) {
			String line = values.next().toString();
			SpatialObj obj = InputParser.getObjFromLine(line);
			quad.insert(obj, offset);
			obj.write(out);
			offset += obj.size();
		}
		output.collect(key, quad);
		out.close();

	}

}

public class QuadTreeIndexer {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(QuadTreeIndexer.class);

		// TODO: specify a mapper and a reducer
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		// TODO: specify output types
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(QuadTree.class);

		conf.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(conf, new Path("src"));
		FileOutputFormat.setOutputPath(conf, new Path("out"));

		//conf.setInputFormat(io.ObjectPositionInputFormat.class);
		conf.setOutputFormat(io.QuadTreeFileOutputFormat.class);

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

