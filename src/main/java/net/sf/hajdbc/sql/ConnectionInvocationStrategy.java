/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2007 Paul Ferraro
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

import java.sql.Connection;
import java.sql.SQLException;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.util.reflect.ProxyFactory;

/**
 * @author Paul Ferraro
 * @param <D> 
 * @param <P> 
 */
public class ConnectionInvocationStrategy<Z, D extends Database<Z>, P> extends DatabaseWriteInvocationStrategy<Z, D, P, Connection, SQLException>
{
	private P connectionFactory;
	private TransactionContext<Z, D> transactionContext;
	
	/**
	 * @param cluster 
	 * @param connectionFactory the factory from which to create connections
	 * @param transactionContext 
	 */
	public ConnectionInvocationStrategy(DatabaseCluster<Z, D> cluster, P connectionFactory, TransactionContext<Z, D> transactionContext)
	{
		super(cluster.getNonTransactionalExecutor());
		
		this.connectionFactory = connectionFactory;
		this.transactionContext = transactionContext;
	}

	/**
	 * @see net.sf.hajdbc.sql.DatabaseWriteInvocationStrategy#invoke(net.sf.hajdbc.sql.SQLProxy, net.sf.hajdbc.sql.Invoker)
	 */
	@Override
	public Connection invoke(SQLProxy<Z, D, P, SQLException> proxy, Invoker<Z, D, P, Connection, SQLException> invoker) throws SQLException
	{
		return ProxyFactory.createProxy(Connection.class, new ConnectionInvocationHandler<Z, D, P>(this.connectionFactory, proxy, invoker, this.invokeAll(proxy, invoker), this.transactionContext));
	}
}
