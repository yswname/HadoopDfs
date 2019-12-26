package cn.com.dfs.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    // 统计每个单词的出现次数
	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1,//[1,3,2,1]
			Reducer<Text, IntWritable, Text, IntWritable>.Context arg2) throws IOException, InterruptedException {
		int sum = 0;
		for(IntWritable num:arg1) {
			sum += num.get();
		}
		arg2.write(arg0, new IntWritable(sum));
	}

}
