package net.sf.ha.jdbc.pool.xa;

import java.sql.SQLException;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.XADataSource;

import net.sf.ha.jdbc.pool.ConnectionPoolDataSourceOperation;

/**
 * @author Paul Ferraro
 * @version $Revision$
 */
public abstract class XADataSourceOperation extends ConnectionPoolDataSourceOperation
{
	public abstract Object execute(XADataSource dataSource) throws SQLException;
	
	/**
	 * @see net.sf.hajdbc.pool.ConnectionPoolDataSourceProxy.Operation#execute(net.sf.hajdbc.DataSourceConnectionInfo, javax.sql.ConnectionPoolDataSource)
	 */
	public final Object execute(ConnectionPoolDataSource dataSource) throws SQLException
	{
		return this.execute((XADataSource) dataSource);
	}
}