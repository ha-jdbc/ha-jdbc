package net.sf.ha.jdbc.pool;

import java.sql.SQLException;

import javax.sql.PooledConnection;

import net.sf.ha.jdbc.Database;
import net.sf.ha.jdbc.Operation;

/**
 * @author Paul Ferraro
 * @version $Revision$
 */
public abstract class PooledConnectionOperation implements Operation
{
	public abstract Object execute(PooledConnection connection) throws SQLException;

	public final Object execute(Database database, Object connection) throws SQLException
	{
		return this.execute((PooledConnection) connection);
	}
}