package net.sf.ha.jdbc;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Paul Ferraro
 * @version $Revision$
 */
public abstract class StatementOperation implements Operation
{
	/**
	 * @param statement
	 * @return
	 * @throws SQLException
	 */
	public abstract Object execute(Statement statement) throws SQLException;

	/**
	 * @see net.sf.ha.jdbc.Operation#execute(net.sf.ha.jdbc.Database, java.lang.Object)
	 */
	public Object execute(Database database, Object object) throws SQLException
	{
		return this.execute((Statement) object);
	}
}