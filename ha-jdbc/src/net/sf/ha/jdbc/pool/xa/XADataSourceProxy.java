package net.sf.ha.jdbc.pool.xa;

import java.sql.SQLException;
import java.util.Map;

import javax.sql.XAConnection;
import javax.sql.XADataSource;

import net.sf.ha.jdbc.DataSourceDatabase;
import net.sf.ha.jdbc.pool.ConnectionPoolDataSourceProxy;

/**
 * @author Paul Ferraro
 * @version $Revision$
 */
public class XADataSourceProxy extends ConnectionPoolDataSourceProxy implements XADataSource
{
	/**
	 * Constructs a new XADataSourceProxy.
	 * @param databaseMap
	 */
	protected XADataSourceProxy(Map databaseMap)
	{
		super(databaseMap);
	}

	/**
	 * @see javax.sql.XADataSource#getXAConnection()
	 */
	public XAConnection getXAConnection() throws SQLException
	{
		XADataSourceOperation operation = new XADataSourceOperation()
		{
			public Object execute(DataSourceDatabase database, XADataSource dataSource) throws SQLException
			{
				return dataSource.getXAConnection();
			}
		};
		
		return new XAConnectionProxy(this, this.executeWrite(operation));
	}

	/**
	 * @see javax.sql.XADataSource#getXAConnection(java.lang.String, java.lang.String)
	 */
	public XAConnection getXAConnection(String user, String password) throws SQLException
	{
		XADataSourceOperation operation = new XADataSourceOperation()
		{
			public Object execute(DataSourceDatabase database, XADataSource dataSource) throws SQLException
			{
				return dataSource.getXAConnection(database.getUser(), database.getPassword());
			}
		};
		
		return new XAConnectionProxy(this, this.executeWrite(operation));
	}	
}
