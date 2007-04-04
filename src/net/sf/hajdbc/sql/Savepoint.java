/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2006 Paul Ferraro
 * 
 * This library is free software; you can redistribute it and/or modify it 
 * under the terms of the GNU Lesser General Public License as published by the 
 * Free Software Foundation; either version 2.1 of the License, or (at your 
 * option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License 
 * for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation, 
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 * 
 * Contact: ferraro@users.sourceforge.net
 */
package net.sf.hajdbc.sql;

import java.sql.SQLException;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.Operation;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class Savepoint extends SQLObject<java.sql.Savepoint, java.sql.Connection> implements java.sql.Savepoint
{
	/**
	 * Constructs a new SavepointProxy.
	 * @param connection
	 * @param operation
	 * @throws SQLException
	 */
	public Savepoint(Connection<?> connection, Operation<java.sql.Connection, java.sql.Savepoint> operation) throws SQLException
	{
		super(connection, operation, connection.getDatabaseCluster().getTransactionalExecutor(), connection.getDatabaseCluster().readLock());
	}
	
	/**
	 * @see java.sql.Savepoint#getSavepointId()
	 */
	public int getSavepointId() throws SQLException
	{
		Operation<java.sql.Savepoint, Integer> operation = new Operation<java.sql.Savepoint, Integer>()
		{
			public Integer execute(Database database, java.sql.Savepoint savepoint) throws SQLException
			{
				return savepoint.getSavepointId();
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.Savepoint#getSavepointName()
	 */
	public String getSavepointName() throws SQLException
	{
		Operation<java.sql.Savepoint, String> operation = new Operation<java.sql.Savepoint, String>()
		{
			public String execute(Database database, java.sql.Savepoint savepoint) throws SQLException
			{
				return savepoint.getSavepointName();
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see net.sf.hajdbc.sql.SQLObject#close(java.lang.Object)
	 */
	@Override
	protected void close(java.sql.Connection connection, java.sql.Savepoint savepoint) throws SQLException
	{
		connection.releaseSavepoint(savepoint);
	}
}
