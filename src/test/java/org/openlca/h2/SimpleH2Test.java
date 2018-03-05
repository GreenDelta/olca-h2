package org.openlca.h2;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
		HikariDataSource pool = createPool();
		try (Connection con = pool.getConnection()) {
			setupTable(con);
		}
		try (Connection con = pool.getConnection()) {
			checkValues(con);
		}
		pool.close();
	}

	@Test
	public void testParallelConnections() throws Exception {
		HikariDataSource pool = createPool();
		try (Connection con = pool.getConnection()) {
			setupTable(con);
		}
		ExecutorService exec = Executors.newFixedThreadPool(16);
		for (int i = 0; i < 1_000_000; i++ ) {
			exec.execute(() -> {
				try (Connection con = pool.getConnection()) {
					checkValues(con);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			});
		}
		exec.shutdown();
		exec.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
	}

	private HikariDataSource createPool() {
		HikariDataSource pool = new HikariDataSource();
		pool.setJdbcUrl(url);
		pool.setUsername("sa");
		pool.setPassword("");
		return pool;
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
