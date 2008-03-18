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
import java.sql.PreparedStatement;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.util.reflect.ProxyFactory;

/**
 * @author Paul Ferraro
 * @param <D> 
 */
public class PreparedStatementInvocationStrategy<D> extends DatabaseWriteInvocationStrategy<D, Connection, PreparedStatement>
{
	private Connection connection;
	private TransactionContext<D> transactionContext;
	private FileSupport fileSupport;
	private String sql;
	
	/**
	 * @param cluster
	 * @param connection
	 * @param transactionContext 
	 * @param fileSupport
	 * @param sql
	 */
	public PreparedStatementInvocationStrategy(DatabaseCluster<D> cluster, Connection connection, TransactionContext<D> transactionContext, FileSupport fileSupport, String sql)
	{
		super(cluster.getNonTransactionalExecutor());
		
		this.connection = connection;
		this.transactionContext = transactionContext;
		this.fileSupport = fileSupport;
		this.sql = sql;
	}

	/**
	 * @see net.sf.hajdbc.sql.DatabaseWriteInvocationStrategy#invoke(net.sf.hajdbc.sql.SQLProxy, net.sf.hajdbc.sql.Invoker)
	 */
	@Override
	public PreparedStatement invoke(SQLProxy<D, Connection> proxy, Invoker<D, Connection, PreparedStatement> invoker) throws Exception
	{
		return ProxyFactory.createProxy(PreparedStatement.class, new PreparedStatementInvocationHandler<D>(this.connection, proxy, invoker, this.invokeAll(proxy, invoker), this.transactionContext, this.fileSupport, this.sql));
	}
}
