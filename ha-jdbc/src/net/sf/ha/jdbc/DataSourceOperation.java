package net.sf.ha.jdbc;

import java.sql.SQLException;

import javax.sql.DataSource;

/**
 * @author Paul Ferraro
 * @version $Revision$
 */
public abstract class DataSourceOperation implements Operation
{
	public abstract Object execute(Database database, DataSource dataSource) throws SQLException;
	
	public final Object execute(Database database, Object connectionFactory) throws SQLException
	{
		return this.execute(database, (DataSource) connectionFactory);
	}
}