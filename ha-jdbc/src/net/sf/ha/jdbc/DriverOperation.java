package net.sf.ha.jdbc;

import java.sql.SQLException;

/**
 * @author Paul Ferraro
 * @version $Revision$
 */
public abstract class DriverOperation implements Operation
{
	public abstract Object execute(DriverDatabase database, Driver driver) throws SQLException;
	
	public final Object execute(Database database, Object connectionFactory) throws SQLException
	{
		return this.execute((DriverDatabase) database, (Driver) connectionFactory);
	}
}