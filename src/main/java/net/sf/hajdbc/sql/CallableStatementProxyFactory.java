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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.util.reflect.Proxies;

/**
 * 
 * @author Paul Ferraro
 */
public class CallableStatementProxyFactory<Z, D extends Database<Z>> extends AbstractPreparedStatementProxyFactory<Z, D, CallableStatement>
{
	public CallableStatementProxyFactory(Connection parent, ProxyFactory<Z, D, Connection, SQLException> parentFactory, Invoker<Z, D, Connection, CallableStatement, SQLException> invoker, Map<D, CallableStatement> map, TransactionContext<Z, D> context, List<Lock> locks)
	{
		super(parent, parentFactory, invoker, map, context, locks, false);
	}

	@Override
	public CallableStatement createProxy()
	{
		return Proxies.createProxy(CallableStatement.class, new CallableStatementInvocationHandler<Z, D>(this));
	}
}
