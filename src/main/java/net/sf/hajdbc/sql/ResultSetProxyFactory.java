/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2013  Paul Ferraro
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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.io.InputSinkRegistry;
import net.sf.hajdbc.util.Resources;
import net.sf.hajdbc.util.reflect.Proxies;

/**
 * 
 * @author Paul Ferraro
 */
public class ResultSetProxyFactory<Z, D extends Database<Z>, S extends Statement> extends AbstractInputSinkRegistryProxyFactory<Z, D, S, ResultSet>
{
	private List<Invoker<Z, D, ResultSet, ?, SQLException>> invokers = new LinkedList<Invoker<Z, D, ResultSet, ?, SQLException>>();
	
	public ResultSetProxyFactory(S statementProxy, ProxyFactory<Z, D, S, SQLException> statementFactory, Invoker<Z, D, S, ResultSet, SQLException> invoker, Map<D, ResultSet> map, TransactionContext<Z, D> context, InputSinkRegistry<Object> sinkRegistry)
	{
		super(statementProxy, statementFactory, invoker, map, context, sinkRegistry);
	}

	public void addInvoker(Invoker<Z, D, ResultSet, ?, SQLException> invoker)
	{
		this.invokers.add(invoker);
	}
	
	public void clearInvokers()
	{
		this.invokers.clear();
	}

	@Override
	public Connection getConnection(D database) throws SQLException
	{
		return this.get(database).getStatement().getConnection();
	}

	@Override
	public void replay(D database, ResultSet results) throws SQLException
	{
		super.replay(database, results);

		for (Invoker<Z, D, ResultSet, ?, SQLException> invoker: this.invokers)
		{
			invoker.invoke(database, results);
		}
	}

	@Override
	public void close(D database, ResultSet results)
	{
		Resources.close(results);
	}

	@Override
	public ResultSet createProxy()
	{
		return Proxies.createProxy(ResultSet.class, new ResultSetInvocationHandler<Z, D, S>(this));
	}
}
