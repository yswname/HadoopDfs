package cn.com.demo.hadoop.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class TestHive {

	public static void main(String[] args) throws Exception{
		test02();
	}
	
	public static void test02()throws Exception{
		Class.forName("org.apache.hive.jdbc.HiveDriver");
		Connection conn = DriverManager.getConnection("jdbc:hive2://192.168.94.100:10000/tp226_db", "root", "yswshj");
		Statement stmt = conn.createStatement();
		String sql = "select * from t_student";
		ResultSet rs = stmt.executeQuery(sql);
		while(rs.next()) {
			System.out.println(rs.getInt(1));
			System.out.println(rs.getString(2));
			System.out.println(rs.getString(3));
			System.out.println(rs.getInt(4));
		}
	}
	
	
	public static void test01()throws Exception{
		Class.forName("org.apache.hive.jdbc.HiveDriver");
		Connection conn = DriverManager.getConnection("jdbc:hive2://192.168.94.100:10000", "root", "yswshj");
		System.out.println(conn);
		Statement stmt = conn.createStatement();
		stmt.execute("show databases");
		ResultSet rs = stmt.getResultSet();
		while(rs.next()) {
			System.out.println(rs.getString(1));
		}
	}

}
