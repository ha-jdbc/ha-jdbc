package net.sf.hajdbc;

import java.io.PrintWriter;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;
import net.sf.hajdbc.cache.DatabaseMetaDataCacheFactoryEnum;
import net.sf.hajdbc.dialect.DialectFactoryEnum;
import net.sf.hajdbc.sql.DataSource;
import net.sf.hajdbc.sql.DataSourceDatabase;
import net.sf.hajdbc.sql.DataSourceDatabaseClusterConfiguration;
import net.sf.hajdbc.sql.SQLProxy;
import net.sf.hajdbc.state.simple.SimpleStateManagerProvider;

import org.junit.Before;

public class Test
{
	private javax.sql.DataSource ds;
	private javax.sql.DataSource ds1;
	private javax.sql.DataSource ds2;
	
	@Before
	public void init() throws Exception
	{
		Class.forName("org.h2.Driver");

		DataSourceDatabase db1 = new DataSourceDatabase();
		db1.setId("db1");
		db1.setName(UrlDataSource.class.getName());
		db1.setProperty("url", "jdbc:h2:mem:db1");
		db1.setUser("sa");
		db1.setPassword("sa");
		
		DataSourceDatabase db2 = new DataSourceDatabase();
		db2.setId("db2");
		db2.setName(UrlDataSource.class.getName());
		db2.setProperty("url", "jdbc:h2:mem:db2");
		db2.setUser("sa");
		db2.setPassword("sa");
		
		DataSourceDatabaseClusterConfiguration config = new DataSourceDatabaseClusterConfiguration();
		
		config.setDatabases(Arrays.asList(db1, db2));
		config.setDialectFactory(DialectFactoryEnum.H2);
		config.setDatabaseMetaDataCacheFactory(DatabaseMetaDataCacheFactoryEnum.NONE);
		config.setStateManagerProvider(new SimpleStateManagerProvider());
		
		DataSource ds = new DataSource();
		ds.setCluster("cluster");
		ds.setConfigurationFactory(new SimpleDatabaseClusterConfigurationFactory<javax.sql.DataSource, DataSourceDatabase>(config));
		
		this.ds = ds;
		
		@SuppressWarnings("unchecked")		
		SQLProxy<javax.sql.DataSource, DataSourceDatabase, javax.sql.DataSource, SQLException> proxy = (SQLProxy<javax.sql.DataSource, DataSourceDatabase, javax.sql.DataSource, SQLException>) Proxy.getInvocationHandler(ds.getProxy());
		this.ds1 = proxy.getObject(db1);
		this.ds2 = proxy.getObject(db2);
	}
	
	@org.junit.Test
	public void test() throws SQLException
	{
		String createSQL = "CREATE TABLE test (id INTEGER NOT NULL, name VARCHAR NOT NULL, PRIMARY KEY (id))";
		
		Connection c1 = this.ds1.getConnection();
		Statement s1 = c1.createStatement();
		s1.execute(createSQL);
		s1.close();
		
		Connection c2 = this.ds2.getConnection();
		Statement s2 = c2.createStatement();
		s2.execute(createSQL);
		s2.close();
		
		Connection c = this.ds.getConnection("sa", "sa");
		PreparedStatement ps = c.prepareStatement("INSERT INTO test (id, name) VALUES (?, ?)");
		ps.setInt(1, 1);
		ps.setString(2, "1");
		ps.addBatch();
		ps.setInt(1, 2);
		ps.setString(2, "2");
		ps.addBatch();
		ps.executeBatch();
		ps.close();
		
		String selectSQL = "SELECT count(*) FROM test";
		
		s1 = c1.createStatement();
		ResultSet rs1 = s1.executeQuery(selectSQL);
		Assert.assertTrue(rs1.next());
		Assert.assertEquals(2, rs1.getInt(1));
		rs1.close();
		s1.close();
		
		s2 = c2.createStatement();
		ResultSet rs2 = s2.executeQuery(selectSQL);
		Assert.assertTrue(rs2.next());
		Assert.assertEquals(2, rs2.getInt(1));
		rs2.close();
		s2.close();
		
		c.close();
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
	}
}
