package net.sf.ha.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author Paul Ferraro
 * @version $Revision$
 */
public abstract class ConnectionOperation implements Operation
{
	public abstract Object execute(Database database, Connection connection) throws SQLException;

	public final Object execute(Database database, Object connection) throws SQLException
	{
		return this.execute(database, (Connection) connection);
	}
}