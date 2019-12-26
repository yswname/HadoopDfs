package cn.com.dfs.score;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ScoreMapper extends Mapper<Text, Text, Text, DoubleWritable> {
	// 从输入的value中获取分数
	// 输出key-名称 value-》分数
	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		String kmScore = value.toString();
		String[] content = kmScore.split(":");
		int score = Integer.parseInt(content[1]);

		context.write(key, new DoubleWritable(score));
	}

}
