package net.sf.ha.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Paul Ferraro
 * @version $Revision$
 */
public abstract class ResultSetOperation implements Operation
{
	/**
	 * @param resultSet
	 * @return
	 * @throws SQLException
	 */
	public abstract Object execute(ResultSet resultSet) throws SQLException;

	/**
	 * @see net.sf.ha.jdbc.Operation#execute(net.sf.ha.jdbc.Database, java.lang.Object)
	 */
	public Object execute(Database database, Object object) throws SQLException
	{
		return this.execute((ResultSet) object);
	}
}