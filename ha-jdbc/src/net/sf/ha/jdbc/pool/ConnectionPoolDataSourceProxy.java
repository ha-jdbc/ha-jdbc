package net.sf.ha.jdbc.pool;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

import net.sf.ha.jdbc.Database;
import net.sf.ha.jdbc.DataSourceDatabase;
import net.sf.ha.jdbc.DatabaseManager;
import net.sf.ha.jdbc.Operation;

/**
 * @author Paul Ferraro
 * @version $Revision$
 */
public class ConnectionPoolDataSourceProxy extends DatabaseManager implements ConnectionPoolDataSource, Referenceable
{
	/**
	 * Constructs a new ConnectionPoolDataSourceProxy.
	 * @param databaseMap
	 */
	protected ConnectionPoolDataSourceProxy(Map databaseMap)
	{
		super(databaseMap);
	}

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
		
		return new PooledConnectionProxy(this, this.executeWrite(operation));
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
		
		return new PooledConnectionProxy(this, this.executeWrite(operation));
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
		
		return ((Integer) this.executeRead(operation)).intValue();
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
		
		this.executeWrite(operation);
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
		
		return (PrintWriter) this.executeRead(operation);
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
		
		this.executeWrite(operation);
	}

	/**
	 * @see net.sf.hajdbc.DatabaseManager#getConnection(net.sf.hajdbc.ConnectionInfo)
	 */
	protected Connection connect(Database database, Object object) throws SQLException
	{
		DataSourceDatabase info = (DataSourceDatabase) database;
		ConnectionPoolDataSource dataSource = (ConnectionPoolDataSource) object;
		String user = info.getUser();
		
		PooledConnection connection = (user == null) ? dataSource.getPooledConnection() : dataSource.getPooledConnection(user, info.getPassword());
		
		return connection.getConnection();
	}
	
	protected abstract static class ConnectionPoolDataSourceOperation implements Operation
	{
		public abstract Object execute(DataSourceDatabase database, ConnectionPoolDataSource dataSource) throws SQLException;
		
		public final Object execute(Database database, Object connectionFactory) throws SQLException
		{
			return this.execute((DataSourceDatabase) database, (ConnectionPoolDataSource) connectionFactory);
		}
	}

	/**
	 * @see javax.naming.Referenceable#getReference()
	 */
	public Reference getReference() throws NamingException
	{
		throw new NamingException();
	}	
}
