package io.github.codetosurvive.hive.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class JDBCTest {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	public static void main(String[] args) throws Exception {

		Class.forName(driverName);

		Connection connection = DriverManager.getConnection("jdbc:hive2://localhost:10000/mydb", "mac", "");

		Statement stmt = connection.createStatement();
		ResultSet resutSet = stmt.executeQuery("select * from bak");

		while (resutSet.next()) {
			System.out.println("id:" + resutSet.getString(1) + "name:" + resutSet.getString(2));
		}
	}
}
