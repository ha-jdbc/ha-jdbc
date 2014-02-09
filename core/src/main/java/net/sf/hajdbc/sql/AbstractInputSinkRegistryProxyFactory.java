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

import java.sql.SQLException;
import java.util.Map;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.io.InputSinkRegistry;

/**
 * 
 * @author Paul Ferraro
 */
public abstract class AbstractInputSinkRegistryProxyFactory<Z, D extends Database<Z>, P, T> extends AbstractSQLProxyFactory<Z, D, P, T> implements InputSinkRegistryProxyFactory<Z, D, P, T>
{
	private final InputSinkRegistry<Object> sinkRegistry;
	
	protected AbstractInputSinkRegistryProxyFactory(P parent, ProxyFactory<Z, D, P, SQLException> parentMap, Invoker<Z, D, P, T, SQLException> invoker, Map<D, T> map, TransactionContext<Z, D> context, InputSinkRegistry<Object> sinkRegistry)
	{
		super(parent, parentMap, invoker, map, context);
		this.sinkRegistry = sinkRegistry;
	}

	@Override
	public InputSinkRegistry<Object> getInputSinkRegistry()
	{
		return this.sinkRegistry;
	}
}
