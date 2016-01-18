/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.sf.hajdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import net.sf.hajdbc.sql.DataSource;
import net.sf.hajdbc.sql.DataSourceDatabaseClusterConfigurationBuilder;
import net.sf.hajdbc.state.StateManager;
import net.sf.hajdbc.state.StateManagerFactory;
import net.sf.hajdbc.state.bdb.BerkeleyDBStateManagerFactory;
import net.sf.hajdbc.state.simple.SimpleStateManagerFactory;
import net.sf.hajdbc.state.sql.SQLStateManagerFactory;
import net.sf.hajdbc.state.sqlite.SQLiteStateManagerFactory;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SmokeTest
{
	@Before
	public void init()
	{
		System.setProperty(StateManager.CLEAR_LOCAL_STATE, Boolean.toString(true));
	}

	@Test
	public void simple() throws Exception
	{
		test(new SimpleStateManagerFactory());
	}
	
	@Test
	public void sqlite() throws Exception
	{
		SQLiteStateManagerFactory factory = new SQLiteStateManagerFactory();
		factory.setLocationPattern("./target/sqlite/{0}");
		test(factory);
	}
	
	@Test
	public void h2() throws Exception
	{
		SQLStateManagerFactory factory = new SQLStateManagerFactory();
		factory.setUrlPattern("jdbc:h2:./target/h2/{0}");
		factory.setUser("sa");
		factory.setPassword("");
		test(factory);
	}
	
	@Test
	public void hsqldb() throws Exception
	{
		SQLStateManagerFactory factory = new SQLStateManagerFactory();
		factory.setUrlPattern("jdbc:hsqldb:./target/hsqldb/{0}");
		factory.setUser("sa");
		factory.setPassword("");
		test(factory);
	}
	
	@Test
	public void berkeleydb() throws Exception
	{
		BerkeleyDBStateManagerFactory factory = new BerkeleyDBStateManagerFactory();
		factory.setLocationPattern("./target/bdb/{0}");
		test(factory);
	}
	
	@Test
	public void derby() throws Exception
	{
		SQLStateManagerFactory factory = new SQLStateManagerFactory();
		factory.setUrlPattern("jdbc:derby:./target/derby/{0};create=true");
		test(factory);
	}
	
	private static void test(StateManagerFactory factory) throws Exception
	{
		JdbcDataSource ds1 = new JdbcDataSource();
		ds1.setUrl("jdbc:h2:mem:db1");
		ds1.setUser("sa");
		ds1.setPassword("");

		JdbcDataSource ds2 = new JdbcDataSource();
		ds2.setUrl("jdbc:h2:mem:db2");
		ds2.setUser("sa");
		ds2.setPassword("");

		try (DataSource ds = new DataSource())
		{
			ds.setCluster("cluster");
			DataSourceDatabaseClusterConfigurationBuilder builder = ds.getConfigurationBuilder();
			builder.addDatabase("db1").dataSource(ds1).credentials("sa", "");
			builder.addDatabase("db2").dataSource(ds2).credentials("sa", "");
			builder.addSynchronizationStrategy("passive");
			builder.defaultSynchronizationStrategy("passive");
			builder.dialect("hsqldb");
			builder.metaDataCache("none");
			builder.state(factory);
			builder.durability("fine");
//			builder.distributable("jgroups");

			try (Connection c1 = ds1.getConnection())
			{
				createTable(c1);
				
				try (Connection c2 = ds2.getConnection())
				{
					createTable(c2);
					
					try (Connection c = ds.getConnection())
					{
						c.setAutoCommit(false);
						
						try (PreparedStatement ps = c.prepareStatement("INSERT INTO test (id, name) VALUES (?, ?)"))
						{
							ps.setInt(1, 1);
							ps.setString(2, "1");
							ps.addBatch();
							ps.setInt(1, 2);
							ps.setString(2, "2");
							ps.addBatch();
							ps.executeBatch();
						}
						
						c.commit();
						
						validate(c1);
						validate(c2);
					}
					finally
					{
						dropTable(c2);
					}
				}
				finally
				{
					dropTable(c1);
				}
			}
		}
	}

	private static void createTable(Connection connection) throws SQLException
	{
		execute(connection, "CREATE TABLE test (id INTEGER NOT NULL, name VARCHAR(10) NOT NULL, PRIMARY KEY (id))");
		connection.commit();
	}

	private static void dropTable(Connection connection) throws SQLException
	{
		execute(connection, "DROP TABLE test");
		connection.commit();
	}

	private static void execute(Connection connection, String sql) throws SQLException
	{
		try (Statement statement = connection.createStatement())
		{
			statement.execute(sql);
		}
	}

	private static void validate(Connection connection) throws SQLException
	{
		try (Statement statement = connection.createStatement())
		{
			try (ResultSet results = statement.executeQuery("SELECT id, name FROM test"))
			{
				Assert.assertTrue(results.next());
				Assert.assertEquals(1, results.getInt(1));
				Assert.assertEquals("1", results.getString(2));
				Assert.assertTrue(results.next());
				Assert.assertEquals(2, results.getInt(1));
				Assert.assertEquals("2", results.getString(2));
				Assert.assertFalse(results.next());
			}
		}
	}
}
