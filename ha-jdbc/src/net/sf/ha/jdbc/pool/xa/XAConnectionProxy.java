package net.sf.ha.jdbc.pool.xa;

import java.sql.SQLException;
import java.util.Map;

import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;

import net.sf.ha.jdbc.DatabaseCluster;
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
	public XAConnectionProxy(DatabaseCluster databaseCluster, Map connectionMap)
	{
		super(databaseCluster, connectionMap);
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
}
