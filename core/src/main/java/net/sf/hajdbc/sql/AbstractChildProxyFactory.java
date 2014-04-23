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

import java.util.Map;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.logging.Level;

/**
 * 
 * @author Paul Ferraro
 */
public abstract class AbstractChildProxyFactory<Z, D extends Database<Z>, P, PE extends Exception, T, E extends Exception> extends AbstractProxyFactory<Z, D, PE, T, E> implements ChildProxyFactory<Z, D, P, PE, T, E>
{
	private final P parentProxy;
	private final ProxyFactory<Z, D, P, PE> parent;
	private final Invoker<Z, D, P, T, PE> invoker;
	
	protected AbstractChildProxyFactory(P parentProxy, ProxyFactory<Z, D, P, PE> parent, Invoker<Z, D, P, T, PE> invoker, Map<D, T> map, Class<E> exceptionClass)
	{
		super(parent.getDatabaseCluster(), map, exceptionClass);
		this.parentProxy = parentProxy;
		this.invoker = invoker;
		this.parent = parent;
		this.parent.addChild(this);
	}

	@Override
	protected T create(D database) throws PE
	{
		return this.invoker.invoke(database, this.parent.get(database));
	}

	@Override
	public P getParentProxy()
	{
		return this.parentProxy;
	}

	@Override
	public ProxyFactory<Z, D, P, PE> getParent()
	{
		return this.parent;
	}

	@Override
	public void remove()
	{
		this.parent.removeChild(this);
	}

	@Override
	public synchronized void close(D database)
	{
		for (ChildProxyFactory<Z, D, T, E, ?, ? extends Exception> child: this.children())
		{
			child.close(database);
		}

		T object = this.remove(database);
		
		if (object != null)
		{
			try
			{
				this.close(database, object);
			}
			catch (Exception e)
			{
				this.logger.log(Level.WARN, e);
			}
		}
	}
}
