package net.sf.ha.jdbc.pool.xa;

import java.sql.SQLException;
import java.util.Map;

import javax.sql.PooledConnection;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;

import net.sf.ha.jdbc.DatabaseManager;
import net.sf.ha.jdbc.pool.PooledConnectionProxy;

/**
 * @author Paul Ferraro
 * @version $Revision$
 */
public class XAConnectionProxy extends PooledConnectionProxy implements XAConnection
{
	/**
	 * Constructs a new XAConnectionProxy.
	 * @param driver
	 * @param connectionMap
	 */
	public XAConnectionProxy(DatabaseManager databaseManager, Map connectionMap)
	{
		super(databaseManager, connectionMap);
	}
	
	/**
	 * @see javax.sql.XAConnection#getXAResource()
	 */
	public XAResource getXAResource() throws SQLException
	{
		XAConnectionOperation operation = new XAConnectionOperation()
		{
			public Object execute(XAConnection connection) throws SQLException
			{
				return connection.getXAResource();
			}
		};
		
		return (XAResource) this.executeRead(operation);
	}
	
	protected abstract static class XAConnectionOperation extends PooledConnectionProxy.PooledConnectionOperation
	{
		public abstract Object execute(XAConnection connection) throws SQLException;

		public final Object execute(PooledConnection connection) throws SQLException
		{
			return this.execute((XAConnection) connection);
		}
	}
}
