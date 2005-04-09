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
package net.sf.hajdbc.sql.pool.xa;

import java.sql.SQLException;

import net.sf.hajdbc.ConnectionFactory;
import net.sf.hajdbc.sql.pool.PooledConnection;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class XAConnection extends PooledConnection implements javax.sql.XAConnection
{
	/**
	 * Constructs a new XAConnectionProxy.
	 * @param dataSource an XADataSource proxy
	 * @param operation the operation that will create XAConnections
	 * @throws SQLException if operation execution fails
	 */
	public XAConnection(ConnectionFactory dataSource, XADataSourceOperation operation) throws SQLException
	{
		super(dataSource, operation);
	}
	
	/**
	 * @see javax.sql.XAConnection#getXAResource()
	 */
	public javax.transaction.xa.XAResource getXAResource() throws SQLException
	{
		XAConnectionOperation operation = new XAConnectionOperation()
		{
			public Object execute(javax.sql.XAConnection connection) throws SQLException
			{
				return connection.getXAResource();
			}
		};
		
		return new XAResource(this, operation);
	}
}
