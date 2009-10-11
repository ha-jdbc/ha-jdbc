/*
 * HA-JDBC: High-Availability JDBC
 * Copyright 2004-2009 Paul Ferraro
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
package net.sf.hajdbc.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.util.reflect.ProxyFactory;

/**
 * @author Paul Ferraro
 * @param <D> 
 */
public class PreparedStatementInvocationStrategy<Z, D extends Database<Z>> extends DatabaseWriteInvocationStrategy<Z, D, Connection, PreparedStatement, SQLException>
{
	private Connection connection;
	private TransactionContext<Z, D> transactionContext;
	private String sql;
	
	/**
	 * @param cluster
	 * @param connection
	 * @param transactionContext 
	 * @param sql
	 */
	public PreparedStatementInvocationStrategy(DatabaseCluster<Z, D> cluster, Connection connection, TransactionContext<Z, D> transactionContext, String sql)
	{
		super(cluster.getNonTransactionalExecutor());
		
		this.connection = connection;
		this.transactionContext = transactionContext;
		this.sql = sql;
	}

	/**
	 * @see net.sf.hajdbc.sql.DatabaseWriteInvocationStrategy#invoke(net.sf.hajdbc.sql.SQLProxy, net.sf.hajdbc.sql.Invoker)
	 */
	@Override
	public PreparedStatement invoke(SQLProxy<Z, D, Connection, SQLException> proxy, Invoker<Z, D, Connection, PreparedStatement, SQLException> invoker) throws SQLException
	{
		return ProxyFactory.createProxy(PreparedStatement.class, new PreparedStatementInvocationHandler<Z, D>(this.connection, proxy, invoker, this.invokeAll(proxy, invoker), this.transactionContext, new FileSupportImpl<SQLException>(proxy.getExceptionFactory()), this.sql));
	}
}
