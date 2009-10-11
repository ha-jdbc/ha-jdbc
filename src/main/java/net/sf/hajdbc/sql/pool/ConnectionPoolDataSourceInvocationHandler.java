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

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Set;

import javax.sql.ConnectionPoolDataSource;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.sql.CommonDataSourceInvocationHandler;
import net.sf.hajdbc.sql.InvocationStrategy;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 *
 */
@SuppressWarnings("nls")
public class ConnectionPoolDataSourceInvocationHandler extends CommonDataSourceInvocationHandler<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase>
{
	private static final Set<Method> getPooledConnectionMethodSet = Methods.findMethods(ConnectionPoolDataSource.class, "getPooledConnection");
	
	/**
	 * @param databaseCluster
	 */
	public ConnectionPoolDataSourceInvocationHandler(DatabaseCluster<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase> databaseCluster)
	{
		super(databaseCluster, ConnectionPoolDataSource.class);
	}

	@Override
	protected InvocationStrategy<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase, ConnectionPoolDataSource, ?, SQLException> getInvocationStrategy(ConnectionPoolDataSource dataSource, Method method, Object[] parameters) throws SQLException
	{
		if (getPooledConnectionMethodSet.contains(method))
		{
			return new PooledConnectionInvocationStrategy(this.cluster, dataSource);
		}
		
		return super.getInvocationStrategy(dataSource, method, parameters);
	}
}
