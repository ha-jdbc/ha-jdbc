package net.sf.ha.jdbc.pool;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

import net.sf.ha.jdbc.DataSourceDatabase;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class ConnectionPoolDataSourceDatabase extends DataSourceDatabase
{
	/**
	 * Constructs a new ConnectionPoolDataSourceConnectionInfo.
	 * @param name
	 */
	public ConnectionPoolDataSourceDatabase(String name)
	{
		super(name);
	}

	/**
	 * @see net.sf.ha.jdbc.ConnectionInfo#connect(java.lang.Object)
	 */
	public Connection connect(Object object) throws SQLException
	{
		ConnectionPoolDataSource dataSource = (ConnectionPoolDataSource) object;
		PooledConnection connection = (this.user != null) ? dataSource.getPooledConnection(this.user, this.password) : dataSource.getPooledConnection();
		
		return this.getConnection(connection);
	}
	
	protected Connection getConnection(PooledConnection connection) throws SQLException
	{
		return connection.getConnection();
	}
}
