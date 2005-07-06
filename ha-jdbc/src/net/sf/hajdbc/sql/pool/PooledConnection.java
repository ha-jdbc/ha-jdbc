/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2004 Paul Ferraro
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
package net.sf.hajdbc.sql.pool;

import java.sql.SQLException;

import javax.sql.ConnectionEventListener;

import net.sf.hajdbc.ConnectionFactory;
import net.sf.hajdbc.SQLObject;
import net.sf.hajdbc.sql.Connection;
import net.sf.hajdbc.sql.FileSupportImpl;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class PooledConnection extends SQLObject implements javax.sql.PooledConnection
{
	/**
	 * Constructs a new PooledConnectionProxy.
	 * @param dataSource a ConnectionPoolDataSource proxy
	 * @param operation an operation that will create PooledConnections
	 * @throws SQLException if operation execution fails
	 */
	public PooledConnection(ConnectionFactory dataSource, ConnectionPoolDataSourceOperation operation) throws SQLException
	{
		super(dataSource, operation);
	}
	
	/**
	 * @see javax.sql.PooledConnection#getConnection()
	 */
	public java.sql.Connection getConnection() throws SQLException
	{
		PooledConnectionOperation operation = new PooledConnectionOperation()
		{
			public Object execute(javax.sql.PooledConnection connection) throws SQLException
			{
				return connection.getConnection();
			}
		};
		
		return new Connection(this, operation, new FileSupportImpl());
	}

	/**
	 * @see javax.sql.PooledConnection#close()
	 */
	public void close() throws SQLException
	{
		PooledConnectionOperation operation = new PooledConnectionOperation()
		{
			public Object execute(javax.sql.PooledConnection connection) throws SQLException
			{
				connection.close();
				
				return null;
			}
		};
		
		this.executeWriteToDatabase(operation);
	}

	/**
	 * @see javax.sql.PooledConnection#addConnectionEventListener(javax.sql.ConnectionEventListener)
	 */
	public void addConnectionEventListener(final ConnectionEventListener listener)
	{
		PooledConnectionOperation operation = new PooledConnectionOperation()
		{
			public Object execute(javax.sql.PooledConnection connection)
			{
				connection.addConnectionEventListener(listener);
				
				return null;
			}
		};
		
		try
		{
			this.executeWriteToDriver(operation);
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * @see javax.sql.PooledConnection#removeConnectionEventListener(javax.sql.ConnectionEventListener)
	 */
	public void removeConnectionEventListener(final ConnectionEventListener listener)
	{
		PooledConnectionOperation operation = new PooledConnectionOperation()
		{
			public Object execute(javax.sql.PooledConnection connection)
			{
				connection.removeConnectionEventListener(listener);
				
				return null;
			}
		};
		
		try
		{
			this.executeWriteToDriver(operation);
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}
}
