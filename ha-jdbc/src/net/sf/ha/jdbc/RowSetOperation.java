package net.sf.ha.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.RowSet;

/**
 * @author Paul Ferraro
 * @version $Revision$
 */
public abstract class RowSetOperation extends ResultSetOperation
{
	public abstract Object execute(RowSet rowSet) throws SQLException;
	
	public final Object execute(ResultSet resultSet) throws SQLException
	{
		return this.execute((RowSet) resultSet);
	}
}