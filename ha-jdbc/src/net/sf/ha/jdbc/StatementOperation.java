package net.sf.ha.jdbc;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Paul Ferraro
 * @version $Revision$
 */
public abstract class StatementOperation implements Operation
{
	public abstract Object execute(Statement statement) throws SQLException;

	/**
	 * @see net.sf.hajdbc.AbstractProxy.Operation#execute(net.sf.hajdbc.ConnectionInfo, java.lang.Object)
	 */
	public Object execute(Database database, Object object) throws SQLException
	{
		return this.execute((Statement) object);
	}
}