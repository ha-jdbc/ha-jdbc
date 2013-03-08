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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.io.InputSinkRegistry;

/**
 * 
 * @author Paul Ferraro
 */
public class ResultSetProxyFactoryFactory<Z, D extends Database<Z>, S extends Statement> implements ProxyFactoryFactory<Z, D, S, SQLException, ResultSet, SQLException>
{
	private final TransactionContext<Z, D> context;
	private final InputSinkRegistry<Object> sinkRegistry;
	
	public ResultSetProxyFactoryFactory(TransactionContext<Z, D> context, InputSinkRegistry<Object> sinkRegistry)
	{
		this.context = context;
		this.sinkRegistry = sinkRegistry;
	}
	
	@Override
	public ProxyFactory<Z, D, ResultSet, SQLException> createProxyFactory(S statementProxy, ProxyFactory<Z, D, S, SQLException> statementFactory, Invoker<Z, D, S, ResultSet, SQLException> invoker, Map<D, ResultSet> results)
	{
		return new ResultSetProxyFactory<Z, D, S>(statementProxy, statementFactory, invoker, results, context, sinkRegistry);
	}
}
