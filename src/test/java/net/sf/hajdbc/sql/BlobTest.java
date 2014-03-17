package net.sf.hajdbc.sql;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.Proxy;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import net.sf.hajdbc.SimpleDatabaseClusterConfigurationFactory;
import net.sf.hajdbc.cache.simple.SimpleDatabaseMetaDataCacheFactory;
import net.sf.hajdbc.dialect.hsqldb.HSQLDBDialectFactory;
import net.sf.hajdbc.durability.none.NoDurabilityFactory;
import net.sf.hajdbc.state.simple.SimpleStateManagerFactory;
import net.sf.hajdbc.util.Resources;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.Assert;
import org.junit.Test;

public class BlobTest
{
	@Test
	public void test() throws Exception
	{
		DataSourceDatabase db1 = new DataSourceDatabase();
		db1.setId("db1");
		db1.setLocation(JDBCDataSource.class.getName());
		db1.setProperty("url", "jdbc:hsqldb:mem:db1");
		
		DataSourceDatabase db2 = new DataSourceDatabase();
		db2.setId("db2");
		db2.setLocation(JDBCDataSource.class.getName());
		db2.setProperty("url", "jdbc:hsqldb:mem:db2");
		
		DataSourceDatabaseClusterConfiguration config = new DataSourceDatabaseClusterConfiguration();
		
		config.setDatabases(Arrays.asList(db1, db2));
		config.setDialectFactory(new HSQLDBDialectFactory());
		config.setDatabaseMetaDataCacheFactory(new SimpleDatabaseMetaDataCacheFactory());
		config.setStateManagerFactory(new SimpleStateManagerFactory());
		config.setDurabilityFactory(new NoDurabilityFactory());

		DataSource ds = new DataSource();
		ds.setCluster("cluster");
		ds.setConfigurationFactory(new SimpleDatabaseClusterConfigurationFactory<javax.sql.DataSource, DataSourceDatabase>(config));
		
		try
		{
			Connection c = ds.getConnection();
			try
			{
				c.setAutoCommit(true);
				createTable(c);
				c.setAutoCommit(false);
				
				Assert.assertFalse(c.getMetaData().locatorsUpdateCopy());
				
				ConnectionInvocationHandler<javax.sql.DataSource, DataSourceDatabase, javax.sql.DataSource> handler = (ConnectionInvocationHandler<javax.sql.DataSource, DataSourceDatabase, javax.sql.DataSource>) Proxy.getInvocationHandler(c);
				ConnectionProxyFactory<javax.sql.DataSource, DataSourceDatabase, javax.sql.DataSource> proxyFactory = handler.getProxyFactory();
	
				Connection c1 = proxyFactory.get(db1);
				Connection c2 = proxyFactory.get(db2);
							
				Blob blob = c.createBlob();
				String expected = "test";
				
				Writer writer = new OutputStreamWriter(blob.setBinaryStream(1));
				writer.write(expected);
				writer.flush();
				writer.close();
				
				PreparedStatement ps = c.prepareStatement("INSERT INTO test (id, lob) VALUES (?, ?)");
				try
				{
					ps.setInt(1, 1);
					ps.setBlob(2, blob);
					ps.executeUpdate();
				}
				finally
				{
					Resources.close(ps);
				}
				c.commit();
				blob.free();
	
				validate(c1, expected);
				validate(c2, expected);
				
				ps = c.prepareStatement("SELECT lob FROM test WHERE id = ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
				try
				{
					ps.setInt(1, 1);
					ResultSet results = ps.executeQuery();
					try
					{
						Assert.assertTrue(results.next());
						blob = results.getBlob(1);
						expected = "1234";
						blob.truncate(blob.length());
						
						/* Not supported by HSQLDB
						writer = new OutputStreamWriter(blob.setBinaryStream(1));
						writer.write(expected);
						writer.close();
						*/
						blob.setBytes(1, expected.getBytes());
						
						results.updateRow();
						Assert.assertFalse(results.next());
						c.commit();
						blob.free();
					}
					finally
					{
						Resources.close(results);
					}
				}
				finally
				{
					Resources.close(ps);
				}
				
				validate(c1, expected);
				validate(c2, expected);
			}
			finally
			{
				c.setAutoCommit(true);
				dropTable(c);
				c.close();
			}
		}
		finally
		{
			ds.stop();
		}
	}

	private static void createTable(Connection connection) throws SQLException
	{
		execute(connection, "CREATE TABLE test (id INTEGER NOT NULL, lob BLOB NOT NULL, PRIMARY KEY (id))");
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

	private static void validate(Connection connection, String expected) throws SQLException, IOException
	{
		Statement statement = connection.createStatement();
		try
		{
			ResultSet results = statement.executeQuery("SELECT id, lob FROM test");
			Assert.assertTrue(results.next());
			Assert.assertEquals(1, results.getInt(1));
			Blob blob = results.getBlob(2);
			BufferedReader reader = new BufferedReader(new InputStreamReader(blob.getBinaryStream()));
			Assert.assertEquals(expected, reader.readLine());
			Assert.assertNull(reader.readLine());
			reader.close();
			Assert.assertFalse(results.next());
		}
		finally
		{
			Resources.close(statement);
		}
	}
}
