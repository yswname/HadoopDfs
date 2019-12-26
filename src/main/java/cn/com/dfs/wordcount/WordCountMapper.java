package cn.com.dfs.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text outKey = new Text();
	private IntWritable outValue = new IntWritable();
    // Mapper出每一行的每个单词
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

        // 行
		String line = value.toString();
		String[] words = line.split("[ ,]");
		for(String word:words) {
			outKey.set(word);
			outValue.set(1);
			// 准备给下一个阶段
			context.write(outKey, outValue);
		}
	}

}
