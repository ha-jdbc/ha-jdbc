package net.sf.ha.jdbc.pool.xa;

import java.sql.SQLException;

import javax.sql.PooledConnection;
import javax.sql.XAConnection;

import net.sf.ha.jdbc.pool.PooledConnectionOperation;

/**
 * @author Paul Ferraro
 * @version $Revision$
 */
public abstract class XAConnectionOperation extends PooledConnectionOperation
{
	public abstract Object execute(XAConnection connection) throws SQLException;

	public final Object execute(PooledConnection connection) throws SQLException
	{
		return this.execute((XAConnection) connection);
	}
}