/**
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2006 Paul Ferraro
 * 
 * This library is free software; you can redistribute it and/or modify it 
 * under the terms of the GNU Lesser General Public License as published by the 
 * Free Software Foundation; either version 2.1 of the License, or (at your 
 * option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License 
 * for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation, 
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 * 
 * Contact: ferraro@users.sourceforge.net
 */
package net.sf.hajdbc.sql;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.SQLException;

import javax.naming.NamingException;

import net.sf.hajdbc.DatabaseClusterTestCase;

import org.testng.annotations.Configuration;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit test for {@link TestDataSource}.
 * @author  Paul Ferraro
 * @since   1.1
 */
public class TestDataSource extends DatabaseClusterTestCase implements javax.sql.DataSource
{
	@Override
	@Configuration(beforeTestClass = true)
	public void setUp() throws Exception
	{
		super.setUp();
		
		DataSource dataSource = new DataSource();
		
		dataSource.setCluster("test-datasource-cluster");
		
		this.context.bind("datasource", dataSource);
	}

	@Override
	@Configuration(afterTestClass = true)
	public void tearDown() throws Exception
	{
		this.context.unbind("datasource");
		
		super.tearDown();
	}

	private javax.sql.DataSource getDataSource()
	{
		try
		{
			return javax.sql.DataSource.class.cast(this.context.lookup("datasource"));
		}
		catch (NamingException e)
		{
			assert false : e;
			return null;
		}
	}

	@DataProvider(name = "connect")
	protected Object[][] connectProvider()
	{
		return new Object[][] { new Object[] { "", "" } };
	}

	@DataProvider(name = "timeout")
	protected Object[][] timeoutProvider()
	{
		return new Object[][] { new Object[] { 0 } };
	}
	
	@DataProvider(name = "writer")
	protected Object[][] writerProvider()
	{
		return new Object[][] { new Object[] { new PrintWriter(new StringWriter()) } };
	}
	
	/**
	 * @see javax.sql.DataSource#getConnection()
	 */
	@Test
	public Connection getConnection() throws SQLException
	{
		Connection connection = this.getDataSource().getConnection();
		
		assert connection != null;
		
		assert net.sf.hajdbc.sql.Connection.class.equals(connection.getClass()) : connection.getClass().getName();
		
		return connection;
	}
	
	/**
	 * @see javax.sql.DataSource#getConnection(java.lang.String, java.lang.String)
	 */
	@Test(dataProvider = "connect")
	public Connection getConnection(String username, String password) throws SQLException
	{
		Connection connection = this.getDataSource().getConnection(username, password);
		
		assert connection != null;
		
		assert net.sf.hajdbc.sql.Connection.class.equals(connection.getClass()) : connection.getClass().getName();
		
		return connection;
	}

	/**
	 * @see javax.sql.DataSource#getLoginTimeout()
	 */
	@Test
	public int getLoginTimeout() throws SQLException
	{
		int timeout = this.getDataSource().getLoginTimeout();
		
		assert timeout == 0 : timeout;
		
		return timeout;
	}

	/**
	 * @see javax.sql.DataSource#getLogWriter()
	 */
	@Test
	public PrintWriter getLogWriter() throws SQLException
	{
		PrintWriter writer = this.getDataSource().getLogWriter();
		
		assert writer != null;
		
		return writer;
	}

	/**
	 * @see javax.sql.DataSource#setLoginTimeout(int)
	 */
	@Test(dataProvider = "timeout")
	public void setLoginTimeout(int seconds) throws SQLException
	{
		this.getDataSource().setLoginTimeout(seconds);
	}

	/**
	 * @see javax.sql.DataSource#setLogWriter(java.io.PrintWriter)
	 */
	@Test(dataProvider = "writer")
	public void setLogWriter(PrintWriter out) throws SQLException
	{
		this.getDataSource().setLogWriter(out);
	}
}
