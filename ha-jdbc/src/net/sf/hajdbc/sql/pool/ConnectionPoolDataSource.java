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

import java.io.PrintWriter;
import java.sql.SQLException;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.Operation;
import net.sf.hajdbc.sql.AbstractDataSource;
import net.sf.hajdbc.sql.AbstractDataSourceFactory;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class ConnectionPoolDataSource extends AbstractDataSource implements javax.sql.ConnectionPoolDataSource
{
	/**
	 * @see javax.sql.ConnectionPoolDataSource#getPooledConnection()
	 */
	public javax.sql.PooledConnection getPooledConnection() throws SQLException
	{
		Operation<javax.sql.ConnectionPoolDataSource, javax.sql.PooledConnection> operation = new Operation<javax.sql.ConnectionPoolDataSource, javax.sql.PooledConnection>()
		{
			public javax.sql.PooledConnection execute(Database database, javax.sql.ConnectionPoolDataSource dataSource) throws SQLException
			{
				return dataSource.getPooledConnection();
			}
		};
		
		return new PooledConnection<javax.sql.PooledConnection, javax.sql.ConnectionPoolDataSource>(this.connectionFactory, operation);
	}

	/**
	 * @see javax.sql.ConnectionPoolDataSource#getPooledConnection(java.lang.String, java.lang.String)
	 */
	public javax.sql.PooledConnection getPooledConnection(final String user, final String password) throws SQLException
	{
		Operation<javax.sql.ConnectionPoolDataSource, javax.sql.PooledConnection> operation = new Operation<javax.sql.ConnectionPoolDataSource, javax.sql.PooledConnection>()
		{
			public javax.sql.PooledConnection execute(Database database, javax.sql.ConnectionPoolDataSource dataSource) throws SQLException
			{
				return dataSource.getPooledConnection(user, password);
			}
		};
		
		return new PooledConnection<javax.sql.PooledConnection, javax.sql.ConnectionPoolDataSource>(this.connectionFactory, operation);
	}

	/**
	 * @see javax.sql.ConnectionPoolDataSource#getLoginTimeout()
	 */
	public int getLoginTimeout() throws SQLException
	{
		Operation<javax.sql.ConnectionPoolDataSource, Integer> operation = new Operation<javax.sql.ConnectionPoolDataSource, Integer>()
		{
			public Integer execute(Database database, javax.sql.ConnectionPoolDataSource dataSource) throws SQLException
			{
				return dataSource.getLoginTimeout();
			}
		};
		
		return (Integer) this.connectionFactory.executeReadFromDriver(operation);
	}

	/**
	 * @see javax.sql.ConnectionPoolDataSource#setLoginTimeout(int)
	 */
	public void setLoginTimeout(final int seconds) throws SQLException
	{
		Operation<javax.sql.ConnectionPoolDataSource, Void> operation = new Operation<javax.sql.ConnectionPoolDataSource, Void>()
		{
			public Void execute(Database database, javax.sql.ConnectionPoolDataSource dataSource) throws SQLException
			{
				dataSource.setLoginTimeout(seconds);
				
				return null;
			}
		};
		
		this.connectionFactory.executeWriteToDriver(operation);
	}

	/**
	 * @see javax.sql.ConnectionPoolDataSource#getLogWriter()
	 */
	public PrintWriter getLogWriter() throws SQLException
	{
		Operation<javax.sql.ConnectionPoolDataSource, PrintWriter> operation = new Operation<javax.sql.ConnectionPoolDataSource, PrintWriter>()
		{
			public PrintWriter execute(Database database, javax.sql.ConnectionPoolDataSource dataSource) throws SQLException
			{
				return dataSource.getLogWriter();
			}
		};
		
		return (PrintWriter) this.connectionFactory.executeReadFromDriver(operation);
	}

	/**
	 * @see javax.sql.ConnectionPoolDataSource#setLogWriter(java.io.PrintWriter)
	 */
	public void setLogWriter(final PrintWriter writer) throws SQLException
	{
		Operation<javax.sql.ConnectionPoolDataSource, Void> operation = new Operation<javax.sql.ConnectionPoolDataSource, Void>()
		{
			public Void execute(Database database, javax.sql.ConnectionPoolDataSource dataSource) throws SQLException
			{
				dataSource.setLogWriter(writer);
				
				return null;
			}
		};
		
		this.connectionFactory.executeWriteToDriver(operation);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractDataSource#getObjectFactoryClass()
	 */
	protected Class<? extends AbstractDataSourceFactory> getObjectFactoryClass()
	{
		return ConnectionPoolDataSourceFactory.class;
	}
}
