package net.sf.ha.jdbc.pool;

import java.io.PrintWriter;
import java.sql.SQLException;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

import net.sf.ha.jdbc.AbstractDataSourceProxy;
import net.sf.ha.jdbc.DataSourceDatabase;

/**
 * @author Paul Ferraro
 * @version $Revision$
 */
public class ConnectionPoolDataSourceProxy extends AbstractDataSourceProxy implements ConnectionPoolDataSource
{
	/**
	 * @see javax.sql.ConnectionPoolDataSource#getPooledConnection()
	 */
	public PooledConnection getPooledConnection() throws SQLException
	{
		ConnectionPoolDataSourceOperation operation = new ConnectionPoolDataSourceOperation()
		{
			public Object execute(DataSourceDatabase database, ConnectionPoolDataSource dataSource) throws SQLException
			{
				return dataSource.getPooledConnection();
			}
		};
		
		return new PooledConnectionProxy(this.databaseCluster, this.databaseCluster.executeWrite(operation));
	}

	/**
	 * @see javax.sql.ConnectionPoolDataSource#getPooledConnection(java.lang.String, java.lang.String)
	 */
	public PooledConnection getPooledConnection(String user, String password) throws SQLException
	{
		ConnectionPoolDataSourceOperation operation = new ConnectionPoolDataSourceOperation()
		{
			public Object execute(DataSourceDatabase database, ConnectionPoolDataSource dataSource) throws SQLException
			{
				return dataSource.getPooledConnection(database.getUser(), database.getPassword());
			}
		};
		
		return new PooledConnectionProxy(this.databaseCluster, this.databaseCluster.executeWrite(operation));
	}

	/**
	 * @see javax.sql.ConnectionPoolDataSource#getLoginTimeout()
	 */
	public int getLoginTimeout() throws SQLException
	{
		ConnectionPoolDataSourceOperation operation = new ConnectionPoolDataSourceOperation()
		{
			public Object execute(DataSourceDatabase database, ConnectionPoolDataSource dataSource) throws SQLException
			{
				return new Integer(dataSource.getLoginTimeout());
			}
		};
		
		return ((Integer) this.databaseCluster.executeRead(operation)).intValue();
	}

	/**
	 * @see javax.sql.ConnectionPoolDataSource#setLoginTimeout(int)
	 */
	public void setLoginTimeout(final int seconds) throws SQLException
	{
		ConnectionPoolDataSourceOperation operation = new ConnectionPoolDataSourceOperation()
		{
			public Object execute(DataSourceDatabase database, ConnectionPoolDataSource dataSource) throws SQLException
			{
				dataSource.setLoginTimeout(seconds);
				
				return null;
			}
		};
		
		this.databaseCluster.executeWrite(operation);
	}

	/**
	 * @see javax.sql.ConnectionPoolDataSource#getLogWriter()
	 */
	public PrintWriter getLogWriter() throws SQLException
	{
		ConnectionPoolDataSourceOperation operation = new ConnectionPoolDataSourceOperation()
		{
			public Object execute(DataSourceDatabase database, ConnectionPoolDataSource dataSource) throws SQLException
			{
				return dataSource.getLogWriter();
			}
		};
		
		return (PrintWriter) this.databaseCluster.executeRead(operation);
	}

	/**
	 * @see javax.sql.ConnectionPoolDataSource#setLogWriter(java.io.PrintWriter)
	 */
	public void setLogWriter(final PrintWriter writer) throws SQLException
	{
		ConnectionPoolDataSourceOperation operation = new ConnectionPoolDataSourceOperation()
		{
			public Object execute(DataSourceDatabase database, ConnectionPoolDataSource dataSource) throws SQLException
			{
				dataSource.setLogWriter(writer);
				
				return null;
			}
		};
		
		this.databaseCluster.executeWrite(operation);
	}

	/**
	 * @see net.sf.ha.jdbc.AbstractDataSourceProxy#getObjectFactoryClass()
	 */
	protected Class getObjectFactoryClass()
	{
		return ConnectionPoolDataSourceFactory.class;
	}
}
