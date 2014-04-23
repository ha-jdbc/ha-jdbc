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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.Invoker;

/**
 * 
 * @author Paul Ferraro
 */
public abstract class AbstractPreparedStatementProxyFactory<Z, D extends Database<Z>, S extends PreparedStatement> extends AbstractStatementProxyFactory<Z, D, S>
{
	private final List<Lock> locks;
	private final boolean selectForUpdate;
	
	protected AbstractPreparedStatementProxyFactory(Connection parent, ProxyFactory<Z, D, Connection, SQLException> parentFactory, Invoker<Z, D, Connection, S, SQLException> invoker, Map<D, S> map, TransactionContext<Z, D> context, List<Lock> locks, boolean selectForUpdate)
	{
		super(parent, parentFactory, invoker, map, context);
		this.locks = locks;
		this.selectForUpdate = selectForUpdate;
	}

	public List<Lock> getLocks()
	{
		return this.locks;
	}
	
	public boolean isSelectForUpdate()
	{
		return this.selectForUpdate;
	}
}
