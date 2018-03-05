package org.openlca.h2;

import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class SimpleH2Test {

	@Test
	public void testSimpleTable() throws Exception {
		String url = "jdbc:h2:mem:testdb" + System.nanoTime() + ";MODE=MySQL";
		Connection con = DriverManager.getConnection(url, "sa", "");
		Assert.assertTrue(con.isValid(200));

		// create a table
		Statement stmt = con.createStatement();
		stmt.execute("CREATE TABLE test_values ( TEST_VALUES VARCHAR(255) )");
		stmt.close();

		// insert values
		stmt = con.createStatement();
		stmt.execute("INSERT INTO TEST_VALUES (test_values) " +
				"VALUES ('abc'), ('def'), ('hji')");
		stmt.close();

		// query values
		stmt = con.createStatement();
		ResultSet rs = stmt.executeQuery("select * from TEST_VALUES");
		String[] values = {"abc", "def", "hji"};
		int i = 0;
		while (rs.next()) {
			Assert.assertEquals(values[i], rs.getString(1));
			i++;
		}
		Assert.assertEquals(3, i);

		con.close();
		Assert.assertTrue(con.isClosed());
	}

}
