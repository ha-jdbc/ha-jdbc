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
package net.sf.hajdbc.sql.io;

import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.sql.AbstractChildProxyFactory;
import net.sf.hajdbc.sql.ProxyFactory;

/**
 * Base proxy factory for IO proxies.
 * @author Paul Ferraro
 * @param <Z> connection source
 * @param <D> database
 * @param <P> parent type
 * @param <T> IO type
 */
public abstract class OutputProxyFactory<Z, D extends Database<Z>, P, T> extends AbstractChildProxyFactory<Z, D, P, SQLException, T, IOException>
{
	private List<Invoker<Z, D, T, ?, IOException>> invokers = new LinkedList<Invoker<Z, D, T, ?, IOException>>();
	
	protected OutputProxyFactory(P parentProxy, ProxyFactory<Z, D, P, SQLException> parent, Invoker<Z, D, P, T, SQLException> invoker, Map<D, T> map)
	{
		super(parentProxy, parent, invoker, map, IOException.class);
	}

	@Override
	public void record(Invoker<Z, D, T, ?, IOException> invoker)
	{
		this.invokers.add(invoker);
	}

	@Override
	public void replay(D database, T output) throws IOException
	{
		for (Invoker<Z, D, T, ?, IOException> invoker: this.invokers)
		{
			invoker.invoke(database, output);
		}
	}
}
