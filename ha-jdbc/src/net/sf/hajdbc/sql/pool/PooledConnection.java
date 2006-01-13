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
package net.sf.hajdbc.sql.pool;

import java.sql.SQLException;

import javax.sql.ConnectionEventListener;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.Operation;
import net.sf.hajdbc.SQLObject;
import net.sf.hajdbc.sql.Connection;
import net.sf.hajdbc.sql.ConnectionFactory;
import net.sf.hajdbc.sql.FileSupportImpl;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @param <E> 
 * @param <P> 
 * @since   1.0
 */
public class PooledConnection<E extends javax.sql.PooledConnection, P> extends SQLObject<E, P> implements javax.sql.PooledConnection
{
	/**
	 * Constructs a new PooledConnectionProxy.
	 * @param dataSource a ConnectionPoolDataSource proxy
	 * @param operation an operation that will create PooledConnections
	 * @throws SQLException if operation execution fails
	 */
	public PooledConnection(ConnectionFactory dataSource, Operation<P, E> operation) throws SQLException
	{
		super(dataSource, operation);
	}
	
	/**
	 * @see javax.sql.PooledConnection#getConnection()
	 */
	public java.sql.Connection getConnection() throws SQLException
	{
		Operation<E, java.sql.Connection> operation = new Operation<E, java.sql.Connection>()
		{
			public java.sql.Connection execute(Database database, E connection) throws SQLException
			{
				return connection.getConnection();
			}
		};
		
		return new Connection<E>(this, operation, new FileSupportImpl());
	}

	/**
	 * @see javax.sql.PooledConnection#close()
	 */
	public void close() throws SQLException
	{
		Operation<E, Void> operation = new Operation<E, Void>()
		{
			public Void execute(Database database, E connection) throws SQLException
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
		Operation<E, Void> operation = new Operation<E, Void>()
		{
			public Void execute(Database database, E connection) throws SQLException
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
		Operation<E, Void> operation = new Operation<E, Void>()
		{
			public Void execute(Database database, E connection) throws SQLException
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
