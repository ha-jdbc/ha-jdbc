/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.sf.hajdbc.pool.sql;

import java.sql.Connection;

import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import javax.sql.StatementEventListener;

/**
 * @author Paul Ferraro
 */
public class DriverPooledConnection implements PooledConnection
{
//	private final ConnectionFactory factory;
//	private Connection connection;
	
	public DriverPooledConnection(ConnectionFactory factory)
	{
//		this.factory = factory;
//		this.connection = factory.getConnection();
	}
	
	/**
	 * {@inheritDoc}
	 * @see javax.sql.PooledConnection#addConnectionEventListener(javax.sql.ConnectionEventListener)
	 */
	@Override
	public void addConnectionEventListener(ConnectionEventListener arg0)
	{
		// TODO Auto-generated method stub

	}

	/**
	 * {@inheritDoc}
	 * @see javax.sql.PooledConnection#addStatementEventListener(javax.sql.StatementEventListener)
	 */
	@Override
	public void addStatementEventListener(StatementEventListener arg0)
	{
		// TODO Auto-generated method stub

	}

	/**
	 * {@inheritDoc}
	 * @see javax.sql.PooledConnection#close()
	 */
	@Override
	public void close()
	{
		// TODO Auto-generated method stub

	}

	/**
	 * {@inheritDoc}
	 * @see javax.sql.PooledConnection#getConnection()
	 */
	@Override
	public Connection getConnection()
	{
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see javax.sql.PooledConnection#removeConnectionEventListener(javax.sql.ConnectionEventListener)
	 */
	@Override
	public void removeConnectionEventListener(ConnectionEventListener arg0)
	{
		// TODO Auto-generated method stub

	}

	/**
	 * {@inheritDoc}
	 * @see javax.sql.PooledConnection#removeStatementEventListener(javax.sql.StatementEventListener)
	 */
	@Override
	public void removeStatementEventListener(StatementEventListener arg0)
	{
		// TODO Auto-generated method stub

	}

}
