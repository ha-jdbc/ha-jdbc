package net.sf.ha.jdbc.pool;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;

import net.sf.ha.jdbc.AbstractConnectionProxy;
import net.sf.ha.jdbc.ConnectionProxy;
import net.sf.ha.jdbc.DatabaseManager;

/**
 * @author Paul Ferraro
 * @version $Revision$
 */
public class PooledConnectionProxy extends AbstractConnectionProxy implements PooledConnection
{
	public PooledConnectionProxy(DatabaseManager databaseManager, Map connectionMap)
	{
		super(databaseManager, connectionMap);
	}
	
	/**
	 * @see javax.sql.PooledConnection#getConnection()
	 */
	public Connection getConnection() throws SQLException
	{
		PooledConnectionOperation operation = new PooledConnectionOperation()
		{
			public Object execute(PooledConnection connection) throws SQLException
			{
				return connection.getConnection();
			}
		};
		
		return new ConnectionProxy(this.getDatabaseManager(), this.executeWrite(operation));
	}

	/**
	 * @see javax.sql.PooledConnection#close()
	 */
	public void close() throws SQLException
	{
		PooledConnectionOperation operation = new PooledConnectionOperation()
		{
			public Object execute(PooledConnection connection) throws SQLException
			{
				connection.close();
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.PooledConnection#addConnectionEventListener(javax.sql.ConnectionEventListener)
	 */
	public void addConnectionEventListener(final ConnectionEventListener listener)
	{
		PooledConnectionOperation operation = new PooledConnectionOperation()
		{
			public Object execute(PooledConnection connection)
			{
				connection.addConnectionEventListener(listener);
				
				return null;
			}
		};
		
		try
		{
			this.executeWrite(operation);
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * @see javax.sql.PooledConnection#removeConnectionEventListener(javax.sql.ConnectionEventListener)
	 */
	public void removeConnectionEventListener(final ConnectionEventListener listener)
	{
		PooledConnectionOperation operation = new PooledConnectionOperation()
		{
			public Object execute(PooledConnection connection)
			{
				connection.removeConnectionEventListener(listener);
				
				return null;
			}
		};
		
		try
		{
			this.executeWrite(operation);
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}
}
