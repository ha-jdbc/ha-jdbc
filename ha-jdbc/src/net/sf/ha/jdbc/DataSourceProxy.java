package net.sf.ha.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

/**
 * @author Paul Ferraro
 * @version $Revision$
 */
public class DataSourceProxy extends AbstractDataSourceProxy implements DataSource
{
	/**
	 * @see javax.sql.DataSource#getLoginTimeout()
	 */
	public int getLoginTimeout() throws SQLException
	{
		DataSourceOperation operation = new DataSourceOperation()
		{
			public Object execute(DataSourceDatabase database, DataSource dataSource) throws SQLException
			{
				return new Integer(dataSource.getLoginTimeout());
			}
		};
		
		return ((Integer) this.databaseCluster.executeRead(operation)).intValue();
	}

	/**
	 * @see javax.sql.DataSource#setLoginTimeout(int)
	 */
	public void setLoginTimeout(final int seconds) throws SQLException
	{
		DataSourceOperation operation = new DataSourceOperation()
		{
			public Object execute(DataSourceDatabase database, DataSource dataSource) throws SQLException
			{
				dataSource.setLoginTimeout(seconds);
				
				return null;
			}
		};
		
		this.databaseCluster.executeWrite(operation);
	}

	/**
	 * @see javax.sql.DataSource#getLogWriter()
	 */
	public PrintWriter getLogWriter() throws SQLException
	{
		DataSourceOperation operation = new DataSourceOperation()
		{
			public Object execute(DataSourceDatabase database, DataSource dataSource) throws SQLException
			{
				return dataSource.getLogWriter();
			}
		};
		
		return (PrintWriter) this.databaseCluster.executeRead(operation);
	}

	/**
	 * @see javax.sql.DataSource#setLogWriter(java.io.PrintWriter)
	 */
	public void setLogWriter(final PrintWriter writer) throws SQLException
	{
		DataSourceOperation operation = new DataSourceOperation()
		{
			public Object execute(DataSourceDatabase database, DataSource dataSource) throws SQLException
			{
				dataSource.setLogWriter(writer);
				
				return null;
			}
		};
		
		this.databaseCluster.executeWrite(operation);
	}

	/**
	 * @see javax.sql.DataSource#getConnection()
	 */
	public Connection getConnection() throws SQLException
	{
		DataSourceOperation operation = new DataSourceOperation()
		{
			public Object execute(DataSourceDatabase database, DataSource dataSource) throws SQLException
			{
				return database.connect(dataSource);
			}
		};
		
		return new ConnectionProxy(this.databaseCluster, this.databaseCluster.executeWrite(operation));
	}

	/**
	 * @see javax.sql.DataSource#getConnection(java.lang.String, java.lang.String)
	 */
	public Connection getConnection(String user, String password) throws SQLException
	{
		return this.getConnection();
	}

	/**
	 * @see net.sf.ha.jdbc.AbstractDataSourceProxy#getObjectFactoryClass()
	 */
	protected Class getObjectFactoryClass()
	{
		return DataSourceFactory.class;
	}
}
