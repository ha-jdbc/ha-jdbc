package net.sf.ha.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Paul Ferraro
 * @version $Revision$
 */
public abstract class ResultSetOperation implements Operation
{
	public abstract Object execute(ResultSet resultSet) throws SQLException;

	/**
	 * @see net.sf.hajdbc.AbstractProxy.Operation#execute(net.sf.hajdbc.ConnectionInfo, java.lang.Object)
	 */
	public Object execute(Database database, Object object) throws SQLException
	{
		return this.execute((ResultSet) object);
	}
}