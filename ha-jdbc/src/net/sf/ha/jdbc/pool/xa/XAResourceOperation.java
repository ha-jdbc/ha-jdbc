package net.sf.ha.jdbc.pool.xa;

import java.sql.SQLException;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import net.sf.ha.jdbc.Database;
import net.sf.ha.jdbc.Operation;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public abstract class XAResourceOperation implements Operation
{
	public abstract Object execute(XADataSourceDatabase database, XAResource resource) throws XAException;
	
	/**
	 * @see net.sf.ha.jdbc.Operation#execute(net.sf.ha.jdbc.Database, java.lang.Object)
	 */
	public Object execute(Database database, Object object) throws SQLException
	{
		try
		{
			return this.execute((XADataSourceDatabase) database, (XAResource) object);
		}
		catch (XAException e)
		{
			SQLException exception = new SQLException(e.getMessage());
			exception.initCause(e);
			throw exception;
		}
	}
}
