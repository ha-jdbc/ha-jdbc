/*
 * Copyright (c) 2004, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc;

import java.sql.SQLException;
import java.sql.Savepoint;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public abstract class SavepointOperation implements Operation
{
	public abstract Object execute(Savepoint savepoint) throws SQLException;
	
	/**
	 * @see net.sf.hajdbc.Operation#execute(net.sf.hajdbc.Database, java.lang.Object)
	 */
	public Object execute(Database database, Object sqlObject) throws SQLException
	{
		return this.execute((Savepoint) sqlObject);
	}
}
