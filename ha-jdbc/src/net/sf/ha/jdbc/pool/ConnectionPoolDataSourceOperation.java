package net.sf.ha.jdbc.pool;

import java.sql.SQLException;

import javax.sql.ConnectionPoolDataSource;


import net.sf.ha.jdbc.Database;
import net.sf.ha.jdbc.Operation;

/**
 * @author Paul Ferraro
 * @version $Revision$
 */
public abstract class ConnectionPoolDataSourceOperation implements Operation
{
	public abstract Object execute(ConnectionPoolDataSource dataSource) throws SQLException;
	
	public final Object execute(Database database, Object connectionFactory) throws SQLException
	{
		return this.execute((ConnectionPoolDataSource) connectionFactory);
	}
}