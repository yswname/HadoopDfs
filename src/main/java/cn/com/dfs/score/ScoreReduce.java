package cn.com.dfs.score;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ScoreReduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	@Override
	protected void reduce(Text arg0, Iterable<DoubleWritable> arg1,
			Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context arg2) throws IOException, InterruptedException {
		double sum = 0;
		int count = 0;
		for(DoubleWritable num:arg1) {
			sum += num.get();
			count++;
		}
		sum = sum/count;
		arg2.write(arg0, new DoubleWritable(sum));
	}

}
