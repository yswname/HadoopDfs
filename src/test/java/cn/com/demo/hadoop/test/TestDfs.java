package cn.com.demo.hadoop.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

public class TestDfs {
	private FileSystem fileSystem;
	// 初始化Hadoop的文件系统对象
	@Before
	public void init() throws Exception{
		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS", "hdfs://192.168.94.100:9000");
		System.setProperty("HADOOP_USER_NAME", "root");
		fileSystem = FileSystem.get(configuration);
	}
	@Test
	public void testCreatePath() throws Exception{
		boolean bool = fileSystem.mkdirs(new Path("/demo"));
		System.out.println(bool);
	}
	@Test
	public void testWriteFile()throws Exception{
		Path path = new Path("/demo/test.txt");
		FSDataOutputStream out = fileSystem.create(path);
		out.write("发发发是方式放大萨芬萨官方打法".getBytes());
		out.flush();
		out.close();
	}

}
