/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2008 Paul Ferraro
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

import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.sql.DatabaseWriteInvocationStrategy;
import net.sf.hajdbc.sql.Invoker;
import net.sf.hajdbc.sql.SQLProxy;
import net.sf.hajdbc.util.reflect.ProxyFactory;

/**
 * @author Paul Ferraro
 */
public class PooledConnectionInvocationStrategy extends DatabaseWriteInvocationStrategy<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase, ConnectionPoolDataSource, PooledConnection, SQLException>
{
	private ConnectionPoolDataSource dataSource;
	
	/**
	 * @param cluster 
	 * @param dataSource
	 */
	public PooledConnectionInvocationStrategy(DatabaseCluster<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase> cluster, ConnectionPoolDataSource dataSource)
	{
		super(cluster.getNonTransactionalExecutor());
		
		this.dataSource = dataSource;
	}
	
	@Override
	public PooledConnection invoke(SQLProxy<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase, ConnectionPoolDataSource, SQLException> proxy, Invoker<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase, ConnectionPoolDataSource, PooledConnection, SQLException> invoker) throws SQLException
	{
		return ProxyFactory.createProxy(PooledConnection.class, new PooledConnectionInvocationHandler(this.dataSource, proxy, invoker, this.invokeAll(proxy, invoker)));
	}
}
