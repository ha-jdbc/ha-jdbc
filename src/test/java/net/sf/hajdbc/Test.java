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

import java.io.PrintWriter;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.logging.Logger;

import net.sf.hajdbc.cache.simple.SimpleDatabaseMetaDataCacheFactory;
import net.sf.hajdbc.dialect.hsqldb.HSQLDBDialectFactory;
import net.sf.hajdbc.distributed.jgroups.JGroupsCommandDispatcherFactory;
import net.sf.hajdbc.sql.DataSource;
import net.sf.hajdbc.sql.DataSourceDatabase;
import net.sf.hajdbc.sql.DataSourceDatabaseClusterConfiguration;
import net.sf.hajdbc.sql.SQLProxy;
import net.sf.hajdbc.state.StateManager;
import net.sf.hajdbc.state.StateManagerFactory;
import net.sf.hajdbc.state.bdb.BerkeleyDBStateManagerFactory;
import net.sf.hajdbc.state.simple.SimpleStateManagerFactory;
import net.sf.hajdbc.state.sql.SQLStateManagerFactory;
import net.sf.hajdbc.state.sqlite.SQLiteStateManagerFactory;

import org.junit.Assert;
import org.junit.Before;

public class Test
{
	@Before
	public void init()
	{
		System.setProperty(StateManager.CLEAR_LOCAL_STATE, Boolean.toString(true));
	}

	@org.junit.Test
	public void simple() throws Exception
	{
		this.test(new SimpleStateManagerFactory());
	}
	
	@org.junit.Test
	public void sqlite() throws Exception
	{
		SQLiteStateManagerFactory factory = new SQLiteStateManagerFactory();
		factory.setLocationPattern("target/sqlite/{0}");
		this.test(factory);
	}
	
	@org.junit.Test
	public void h2() throws Exception
	{
		SQLStateManagerFactory factory = new SQLStateManagerFactory();
		factory.setUrlPattern("jdbc:h2:target/h2/{0}");
		this.test(factory);
	}
	
	@org.junit.Test
	public void hsqldb() throws Exception
	{
		SQLStateManagerFactory factory = new SQLStateManagerFactory();
		factory.setUrlPattern("jdbc:hsqldb:target/hsqldb/{0}");
		this.test(factory);
	}
	
	@org.junit.Test
	public void berkeleydb() throws Exception
	{
		BerkeleyDBStateManagerFactory factory = new BerkeleyDBStateManagerFactory();
		factory.setLocationPattern("target/bdb/{0}");
		this.test(factory);
	}
	
	@org.junit.Test
//	@org.junit.Ignore(value = "Figure out why we get OutOfMemoryError on connect")
	public void derby() throws Exception
	{
		SQLStateManagerFactory factory = new SQLStateManagerFactory();
		factory.setUrlPattern("jdbc:derby:target/derby/{0};create=true");
		this.test(factory);
	}
	
	private void test(StateManagerFactory factory) throws Exception
	{
		DataSourceDatabase db1 = new DataSourceDatabase();
		db1.setId("db1");
		db1.setLocation(UrlDataSource.class.getName());
		db1.setProperty("url", "jdbc:hsqldb:mem:db1");
		db1.setUser("sa");
		db1.setPassword("");
		
		DataSourceDatabase db2 = new DataSourceDatabase();
		db2.setId("db2");
		db2.setLocation(UrlDataSource.class.getName());
		db2.setProperty("url", "jdbc:hsqldb:mem:db2");
		db2.setUser("sa");
		db2.setPassword("");
		
		DataSourceDatabaseClusterConfiguration config = new DataSourceDatabaseClusterConfiguration();
		
		config.setDatabases(Arrays.asList(db1, db2));
		config.setDialectFactory(new HSQLDBDialectFactory());
		config.setDatabaseMetaDataCacheFactory(new SimpleDatabaseMetaDataCacheFactory());
		config.setStateManagerFactory(factory);
		config.setDispatcherFactory(new JGroupsCommandDispatcherFactory());

		DataSource ds = new DataSource();
		ds.setCluster("cluster");
		ds.setConfigurationFactory(new SimpleDatabaseClusterConfigurationFactory<javax.sql.DataSource, DataSourceDatabase>(config));
		
		try
		{
			@SuppressWarnings("unchecked")
			SQLProxy<javax.sql.DataSource, DataSourceDatabase, javax.sql.DataSource, SQLException> proxy = (SQLProxy<javax.sql.DataSource, DataSourceDatabase, javax.sql.DataSource, SQLException>) Proxy.getInvocationHandler(ds.getProxy());
			javax.sql.DataSource ds1 = proxy.getObject(db1);
			javax.sql.DataSource ds2 = proxy.getObject(db2);
	
			String createSQL = "CREATE TABLE test (id INTEGER NOT NULL, name VARCHAR(10) NOT NULL, PRIMARY KEY (id))";
			String dropSQL = "DROP TABLE test";
			
			Connection c1 = ds1.getConnection("sa", "");
			try
			{
				Statement s1 = c1.createStatement();
				try
				{
					s1.execute(createSQL);
				}
				finally
				{
					s1.close();
				}
				try
				{
					Connection c2 = ds2.getConnection("sa", "");
					try
					{
						Statement s2 = c2.createStatement();
						try
						{
							s2.execute(createSQL);
						}
						finally
						{
							s2.close();
						}
						
						try
						{
							Connection c = ds.getConnection("sa", "");
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
									ps.close();
								}
								c.commit();
								
								String selectSQL = "SELECT id, name FROM test";
								
								s1 = c1.createStatement();
								try
								{
									ResultSet rs1 = s1.executeQuery(selectSQL);
									Assert.assertTrue(rs1.next());
									Assert.assertEquals(1, rs1.getInt(1));
									Assert.assertEquals("1", rs1.getString(2));
									Assert.assertTrue(rs1.next());
									Assert.assertEquals(2, rs1.getInt(1));
									Assert.assertEquals("2", rs1.getString(2));
									Assert.assertFalse(rs1.next());
								}
								finally
								{
									s1.close();
								}
								s2 = c2.createStatement();
								try
								{
									ResultSet rs2 = s2.executeQuery(selectSQL);
									Assert.assertTrue(rs2.next());
									Assert.assertEquals(1, rs2.getInt(1));
									Assert.assertEquals("1", rs2.getString(2));
									Assert.assertTrue(rs2.next());
									Assert.assertEquals(2, rs2.getInt(1));
									Assert.assertEquals("2", rs2.getString(2));
									Assert.assertFalse(rs2.next());
								}
								finally
								{
									s2.close();
								}
							}
							finally
							{
								c.close();
							}
						}
						finally
						{
							s2 = c2.createStatement();
							try
							{
								s2.executeUpdate(dropSQL);
							}
							finally
							{
								s2.close();
							}
						}
					}
					finally
					{
						c2.close();
					}
				}
				finally
				{
					s1 = c1.createStatement();
					try
					{
						s1.executeUpdate(dropSQL);
					}
					finally
					{
						s1.close();
					}
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
	
	public static class UrlDataSource implements javax.sql.DataSource
	{
		private String url;
		private volatile Connection connection;
		
		public String getUrl()
		{
			return this.url;
		}
		
		public void setUrl(String url)
		{
			this.url = url;
		}
		
		@Override
		public synchronized Connection getConnection() throws SQLException
		{
			if (this.connection == null)
			{
				this.connection = DriverManager.getConnection(this.url);
			}
			
			return this.connection;
		}

		@Override
		public synchronized Connection getConnection(String user, String password) throws SQLException
		{
			if (this.connection == null)
			{
				this.connection = DriverManager.getConnection(this.url, user, password);
			}
			
			return this.connection;
		}

		@Override
		public PrintWriter getLogWriter() throws SQLException
		{
			return null;
		}

		@Override
		public int getLoginTimeout() throws SQLException
		{
			return 0;
		}

		@Override
		public void setLogWriter(PrintWriter arg0) throws SQLException
		{
		}

		@Override
		public void setLoginTimeout(int arg0) throws SQLException
		{
		}

		@Override
		public boolean isWrapperFor(Class<?> arg0) throws SQLException
		{
			return false;
		}

		@Override
		public <T> T unwrap(Class<T> arg0) throws SQLException
		{
			return null;
		}

		@Override
		public Logger getParentLogger() throws SQLFeatureNotSupportedException
		{
			return null;
		}
	}
}
