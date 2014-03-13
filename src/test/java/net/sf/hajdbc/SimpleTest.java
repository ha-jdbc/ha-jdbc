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

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import net.sf.hajdbc.cache.simple.SimpleDatabaseMetaDataCacheFactory;
import net.sf.hajdbc.dialect.hsqldb.HSQLDBDialectFactory;
import net.sf.hajdbc.durability.fine.FineDurabilityFactory;
import net.sf.hajdbc.sql.DataSource;
import net.sf.hajdbc.sql.DataSourceDatabase;
import net.sf.hajdbc.sql.DataSourceDatabaseClusterConfiguration;
import net.sf.hajdbc.sql.InvocationHandler;
import net.sf.hajdbc.sql.ProxyFactory;
import net.sf.hajdbc.state.StateManager;
import net.sf.hajdbc.state.StateManagerFactory;
import net.sf.hajdbc.state.bdb.BerkeleyDBStateManagerFactory;
import net.sf.hajdbc.state.simple.SimpleStateManagerFactory;
import net.sf.hajdbc.state.sql.SQLStateManagerFactory;
import net.sf.hajdbc.state.sqlite.SQLiteStateManagerFactory;
import net.sf.hajdbc.util.Resources;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SimpleTest
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
		factory.setLocationPattern("target/sqlite/{0}");
		test(factory);
	}
	
	@Test
	public void h2() throws Exception
	{
		SQLStateManagerFactory factory = new SQLStateManagerFactory();
		factory.setUrlPattern("jdbc:h2:target/h2/{0}");
		test(factory);
	}
	
	@Test
	public void hsqldb() throws Exception
	{
		SQLStateManagerFactory factory = new SQLStateManagerFactory();
		factory.setUrlPattern("jdbc:hsqldb:target/hsqldb/{0}");
		test(factory);
	}
	
	@Test
	public void berkeleydb() throws Exception
	{
		BerkeleyDBStateManagerFactory factory = new BerkeleyDBStateManagerFactory();
		factory.setLocationPattern("target/bdb/{0}");
		test(factory);
	}
	
	@Test
	public void derby() throws Exception
	{
		SQLStateManagerFactory factory = new SQLStateManagerFactory();
		factory.setUrlPattern("jdbc:derby:target/derby/{0};create=true");
		test(factory);
	}
	
	private static void test(StateManagerFactory factory) throws Exception
	{
		DataSourceDatabase db1 = new DataSourceDatabase();
		db1.setId("db1");
		db1.setLocation(JDBCDataSource.class.getName());
		db1.setProperty("url", "jdbc:hsqldb:mem:db1");
		db1.setProperty("user", "sa");
		db1.setProperty("password", "");
		db1.setUser("sa");
		db1.setPassword("");
		
		DataSourceDatabase db2 = new DataSourceDatabase();
		db2.setId("db2");
		db2.setLocation(JDBCDataSource.class.getName());
		db2.setProperty("url", "jdbc:hsqldb:mem:db2");
		db2.setProperty("user", "sa");
		db2.setProperty("password", "");
		db2.setUser("sa");
		db2.setPassword("");
		
		DataSourceDatabaseClusterConfiguration config = new DataSourceDatabaseClusterConfiguration();
		
		config.setDatabases(Arrays.asList(db1, db2));
		config.setDialectFactory(new HSQLDBDialectFactory());
		config.setDatabaseMetaDataCacheFactory(new SimpleDatabaseMetaDataCacheFactory());
		config.setStateManagerFactory(factory);
//		config.setDispatcherFactory(new JGroupsCommandDispatcherFactory());
		config.setDurabilityFactory(new FineDurabilityFactory());

		DataSource ds = new DataSource();
		ds.setCluster("cluster");
		ds.setConfigurationFactory(new SimpleDatabaseClusterConfigurationFactory<javax.sql.DataSource, DataSourceDatabase>(config));
		
		InvocationHandler<javax.sql.DataSource, DataSourceDatabase, javax.sql.DataSource, SQLException, ProxyFactory<javax.sql.DataSource, DataSourceDatabase, javax.sql.DataSource, SQLException>> handler = (InvocationHandler<javax.sql.DataSource, DataSourceDatabase, javax.sql.DataSource, SQLException, ProxyFactory<javax.sql.DataSource, DataSourceDatabase, javax.sql.DataSource, SQLException>>) Proxy.getInvocationHandler(ds.getProxy());
		ProxyFactory<javax.sql.DataSource, DataSourceDatabase, javax.sql.DataSource, SQLException> proxyFactory = handler.getProxyFactory();

		try
		{
			Connection c1 = proxyFactory.get(db1).getConnection();
			try
			{
				createTable(c1);
				
				try
				{
					Connection c2 = proxyFactory.get(db2).getConnection();
					try
					{
						createTable(c2);
						
						try
						{
							Connection c = ds.getConnection();
							try
							{
								c.setAutoCommit(false);
								PreparedStatement ps = c.prepareStatement("INSERT INTO test (id, name) VALUES (?, ?)");
								try
								{
									ps.setInt(1, 1);
									ps.setString(2, "1");
									ps.addBatch();
									ps.setInt(1, 2);
									ps.setString(2, "2");
									ps.addBatch();
									ps.executeBatch();
								}
								finally
								{
									Resources.close(ps);
								}
								c.commit();
								
								validate(c1);
								validate(c2);
							}
							finally
							{
								Resources.close(c);
							}
						}
						finally
						{
							dropTable(c2);
						}
					}
					finally
					{
						c2.close();
					}
				}
				finally
				{
					dropTable(c1);
				}
			}
			finally
			{
				c1.close();
			}
		}
		finally
		{
			ds.stop();
		}
	}

	private static void createTable(Connection connection) throws SQLException
	{
		execute(connection, "CREATE TABLE test (id INTEGER NOT NULL, name VARCHAR(10) NOT NULL, PRIMARY KEY (id))");
	}

	private static void dropTable(Connection connection) throws SQLException
	{
		execute(connection, "DROP TABLE test");
	}

	private static void execute(Connection connection, String sql) throws SQLException
	{
		Statement statement = connection.createStatement();
		try
		{
			statement.execute(sql);
		}
		finally
		{
			Resources.close(statement);
		}
	}

	private static void validate(Connection connection) throws SQLException
	{
		Statement statement = connection.createStatement();
		try
		{
			ResultSet results = statement.executeQuery("SELECT id, name FROM test");
			Assert.assertTrue(results.next());
			Assert.assertEquals(1, results.getInt(1));
			Assert.assertEquals("1", results.getString(2));
			Assert.assertTrue(results.next());
			Assert.assertEquals(2, results.getInt(1));
			Assert.assertEquals("2", results.getString(2));
			Assert.assertFalse(results.next());
		}
		finally
		{
			Resources.close(statement);
		}
	}
}
