package cn.com.dfs.score;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ScoreDriver {

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "My WordCount Demo Job");
		
		job.setJarByClass(ScoreDriver.class);
		job.setInputFormatClass(ScoreInputFormat.class);
		job.setMapperClass(ScoreMapper.class);
		job.setReducerClass(ScoreReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("/demo/score/input"));
		FileOutputFormat.setOutputPath(job, new Path("/demo/score/output"));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
