package net.sf.ha.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Paul Ferraro
 * @version $Revision$
 */
public abstract class PreparedStatementOperation extends StatementOperation
{
	public abstract Object execute(PreparedStatement statement) throws SQLException;
	
	public final Object execute(Statement statement) throws SQLException
	{
		return this.execute((PreparedStatement) statement);
	}
}