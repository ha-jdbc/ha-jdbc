package net.sf.ha.jdbc;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Paul Ferraro
 * @version $Revision$
 */
public abstract class CallableStatementOperation extends PreparedStatementOperation
{
	public abstract Object execute(CallableStatement statement) throws SQLException;
	
	public final Object execute(PreparedStatement statement) throws SQLException
	{
		return this.execute((CallableStatement) statement);
	}
}