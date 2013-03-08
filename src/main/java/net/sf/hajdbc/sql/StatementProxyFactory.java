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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.util.reflect.Proxies;

/**
 * 
 * @author Paul Ferraro
 */
public class StatementProxyFactory<Z, D extends Database<Z>> extends AbstractStatementProxyFactory<Z, D, Statement>
{
	public StatementProxyFactory(Connection parent, ProxyFactory<Z, D, Connection, SQLException> parentFactory, Invoker<Z, D, Connection, Statement, SQLException> invoker, Map<D, Statement> map, TransactionContext<Z, D> context)
	{
		super(parent, parentFactory, invoker, map, context);
	}

	@Override
	public Statement createProxy()
	{
		return Proxies.createProxy(Statement.class, new StatementInvocationHandler<Z, D>(this));
	}
}
