package mapReduceTasks;

import io.InputParser;

import java.io.IOException;
import java.util.Iterator;

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
import org.apache.hadoop.mapred.TaskAttemptID;

import quadIndex.SpatialObj;

/*
 * This MapReduce Job is to creating quadtree index
 * on Hadoop.
 * Author: Donghan Miao
 * 
 * */
class Map extends MapReduceBase implements
		Mapper<LongWritable, Text, IntWritable, Text> {

	private int NUM_OF_NODES = 1;
	private JobConf myJobConf ;
	private int _attemptID=9;
	
	
	@Override
	public void configure(JobConf conf){
		NUM_OF_NODES = conf.getNumReduceTasks();
		myJobConf = conf;
		TaskAttemptID attempt = TaskAttemptID.forName(myJobConf.get("mapred.task.id"));
		_attemptID = attempt.getTaskID().getId();
		System.out.println("AttemptID =" + _attemptID);
	}
	
	@Override
	public void map(LongWritable key, Text value, // input key, value
			OutputCollector<IntWritable, Text> output, Reporter reporter)
			throws IOException {

		output.collect(new IntWritable(_attemptID), value);
	}

}

class Reduce extends MapReduceBase implements
		Reducer<IntWritable, Text, IntWritable, QuadTreeWritable> {
	private JobConf myJobConf ;

	@Override
	public void	configure(JobConf job) {
		myJobConf = job;
	}
	
	@Override
	public void reduce(IntWritable key, Iterator<Text> values,
			OutputCollector<IntWritable, QuadTreeWritable> output, Reporter reporter)
			throws IOException {

		int num = 0;
		QuadTreeWritable quad = new QuadTreeWritable();

		while (values.hasNext()) {
			String line = values.next().toString();
			SpatialObj obj = InputParser.getObjFromLine(line);
			if(!quad.insert(obj)){
				continue;
			} else {
				output.collect(new IntWritable((key.get()%100)*1000+(num++)), quad);
				quad = new QuadTreeWritable();
				continue;
			}
		}
		if(!quad.isEmpty())
			output.collect(new IntWritable((key.get()%100)*1000+(num++)), quad);
	}

}

public class QuadTreeIndexer {

	public static void main(String[] args) {
		String src = null;
		String out = null;
		if(args.length <2){
			src = "src";
			out = "out";
		} else {
			src = args[0];
			out = args[1];
		}
				
		JobClient client = new JobClient();
		JobConf conf = new JobConf(QuadTreeIndexer.class);

		// TODO: specify a mapper and a reducer
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		// TODO: specify output types
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(QuadTreeWritable.class);

		conf.setNumReduceTasks(conf.getNumReduceTasks());
		System.out.println("Number of Working Machines: "+conf.getNumReduceTasks());
		FileInputFormat.setInputPaths(conf, new Path(src));
		FileOutputFormat.setOutputPath(conf, new Path(out));

		//conf.set("outPath", out);
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

