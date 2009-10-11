/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2009 Paul Ferraro
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
package net.sf.hajdbc.state.sql;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import javax.sql.StatementEventListener;

/**
 * @author paul
 *
 */
public class SQLPooledConnection implements PooledConnection
{

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
	public void close() throws SQLException
	{
		// TODO Auto-generated method stub

	}

	/**
	 * {@inheritDoc}
	 * @see javax.sql.PooledConnection#getConnection()
	 */
	@Override
	public Connection getConnection() throws SQLException
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
