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
	/**
	 * @param dataSource
	 * @return
	 * @throws SQLException
	 */
	public abstract Object execute(XADataSource dataSource) throws SQLException;
	
	/**
	 * @see net.sf.ha.jdbc.pool.ConnectionPoolDataSourceOperation#execute(javax.sql.ConnectionPoolDataSource)
	 */
	public final Object execute(ConnectionPoolDataSource dataSource) throws SQLException
	{
		return this.execute((XADataSource) dataSource);
	}
}