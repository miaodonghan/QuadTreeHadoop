package mapReduceTasks;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
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
import quadIndex.Rectangle;

/*
 * This MapReduce Job is to query spatial Objects
 * On MapReduce
 * Author: Donghan Miao
 * */

class QueryMap extends MapReduceBase implements
		Mapper<IntWritable, QuadTreeWritable, IntWritable, Text> {

	private JobConf myJobConf;

	@Override
	public void configure(JobConf job) {
		myJobConf = job;
	}

	@Override
	public void map(IntWritable key, QuadTreeWritable value, // input key, value
			OutputCollector<IntWritable, Text> output, Reporter reporter)
			throws IOException {

		// Configuration config = new Configuration();
		// config.set("fs.default.name", "hdfs://127.0.0.1:9000/");

		// FileSystem dfs = FileSystem.get(myJobConf);

		// FSDataInputStream in = dfs.open(new Path(dfs.getWorkingDirectory()
		// + "/"+ myJobConf.get("query")));
		// BufferedReader din = new BufferedReader(new InputStreamReader(in));

		// FSDataInputStream RawReader = dfs.open(new
		// Path(dfs.getWorkingDirectory()+
		// "/"+myJobConf.get("out") +"/"+ key.toString() + ".rawdata"));

		String line = myJobConf.get("rect");
		// while (line != null) {
		String[] nums = line.split(",");

		int queryId = Integer.parseInt(nums[0]);
		float rect[] = new float[4];
		for (int i = 0; i < 4; i++) {
			rect[i] = Float.parseFloat(nums[i + 1]);
		}

		Rectangle range = new Rectangle(rect[0], rect[1], rect[2], rect[3]);

		List<Text> result = value.cur_RangeQuery(range);

		for (Text obj : result) {
			output.collect(new IntWritable(queryId), obj);
		}

		// }
	}

}

class QueryReduce extends MapReduceBase implements
		Reducer<IntWritable, Text, IntWritable, Text> {

	@Override
	public void reduce(IntWritable key, Iterator<Text> values,
			OutputCollector<IntWritable, Text> output, Reporter reporter)
			throws IOException {

		StringBuilder T = new StringBuilder();
		while (values.hasNext()) {
			T.append(values.next().toString()+"\n");
		}

		output.collect(key, new Text(T.toString()));
	}
}

class RegexFilter extends Configured implements PathFilter {

	// only files generated from last reducer should be processed.
	String regFilter = ".*part.*";
	Pattern pattern = Pattern.compile(regFilter);
	boolean firstcall = true;

	@Override
	public boolean accept(Path path) {
		if (firstcall) {
			firstcall = false;
			return true;
		}

		Matcher m = pattern.matcher(path.toString());

		System.out.println("Is path : " + path.toString() + " matching "
				+ regFilter + " ? , " + m.matches());
		return m.matches();
	}

}

public class QuadTreeQuery {

	public static void main(String[] args) {
		String out = "out";
		String result = "result";
		String rect = null;
		if (args.length == 1) {
			rect = args[0];
		} else if (args.length == 3) {
			out = args[0];
			result = args[1];
			rect = args[2];
		} else {

		}

		JobClient client = new JobClient();
		JobConf conf = new JobConf(QuadTreeQuery.class);

		conf.set("rect", rect);

		// TODO: specify a mapper and a reducer
		conf.setMapperClass(QueryMap.class);
		conf.setReducerClass(QueryReduce.class);

		// TODO: specify output types
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setNumReduceTasks(conf.getNumReduceTasks());
		System.out.println("Number of Working Machines: "
				+ conf.getNumReduceTasks());

		FileInputFormat.setInputPaths(conf, new Path(out));
		FileOutputFormat.setOutputPath(conf, new Path(result));
		// FileInputFormat.setInputPathFilter(conf, RegexFilter.class);

		conf.setInputFormat(io.QuadTreeInputFormat.class);
		// conf.setOutputFormat(io.QuadTreeFileOutputFormat.class);

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
