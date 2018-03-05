package org.openlca.h2;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class SimpleH2Test {

	private String url = "jdbc:h2:mem:testdb" + System.nanoTime();

	@Test
	public void testSimpleConnection() throws Exception {
		Connection con = DriverManager.getConnection(url, "sa", "");
		Assert.assertTrue(con.isValid(200));
		setupTable(con);
		checkValues(con);
		con.close();
		Assert.assertTrue(con.isClosed());
	}

	@Test
	public void testWithConnectionPool() throws Exception {
		HikariDataSource pool = new HikariDataSource();
		pool.setJdbcUrl(url);
		pool.setUsername("sa");
		pool.setPassword("");
		try (Connection con = pool.getConnection()) {
			setupTable(con);
		}
		try (Connection con = pool.getConnection()) {
			checkValues(con);
		}
		pool.close();
	}

	private void setupTable(Connection con) throws Exception {
		Statement stmt = con.createStatement();
		stmt.execute("CREATE TABLE test_values ( TEST_VALUES VARCHAR(255) )");
		stmt.close();
		stmt = con.createStatement();
		stmt.execute("INSERT INTO TEST_VALUES (test_values) " +
				"VALUES ('abc'), ('def'), ('hji')");
		stmt.close();
	}

	private void checkValues(Connection con) throws Exception {
		Statement stmt = con.createStatement();
		ResultSet rs = stmt.executeQuery("select * from TEST_VALUES");
		String[] values = {"abc", "def", "hji"};
		int i = 0;
		while (rs.next()) {
			Assert.assertEquals(values[i], rs.getString(1));
			i++;
		}
		rs.close();
		con.close();
		Assert.assertEquals(3, i);
	}

}
