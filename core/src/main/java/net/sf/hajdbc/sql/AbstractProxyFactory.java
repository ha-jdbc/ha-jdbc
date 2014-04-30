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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.ExceptionFactory;
import net.sf.hajdbc.ExceptionType;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggerFactory;
import net.sf.hajdbc.messages.Messages;
import net.sf.hajdbc.messages.MessagesFactory;

/**
 * 
 * @author Paul Ferraro
 */
public abstract class AbstractProxyFactory<Z, D extends Database<Z>, TE extends Exception, T, E extends Exception> implements ProxyFactory<Z, D, T, E>
{
	protected Messages messages = MessagesFactory.getMessages();
	protected Logger logger = LoggerFactory.getLogger(this.getClass());
	
	private final DatabaseCluster<Z, D> cluster;
	private final Map<D, T> map;
	private final Set<ChildProxyFactory<Z, D, T, E, ?, ? extends Exception>> children = Collections.newSetFromMap(new WeakHashMap<ChildProxyFactory<Z, D, T, E, ?, ? extends Exception>, Boolean>());
	private final Set<Invoker<Z, D, T, ?, E>> invokers = new HashSet<>();
	private final ExceptionFactory<E> exceptionFactory;
	
	/**
	 * Constructs a new proxy to a set of objects
	 * @param map a map of database to sql object.
	 * @param exceptionClass the class for exceptions thrown by this object
	 */
	protected AbstractProxyFactory(DatabaseCluster<Z, D> cluster, Map<D, T> map, Class<E> exceptionClass)
	{
		this.cluster = cluster;
		this.map = map;
		this.exceptionFactory = ExceptionType.valueOf(exceptionClass).getExceptionFactory();
	}

	@Override
	public DatabaseCluster<Z, D> getDatabaseCluster()
	{
		return this.cluster;
	}

	protected T remove(D database)
	{
		return this.map.remove(database);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<Map.Entry<D, T>> entries()
	{
		return this.map.entrySet();
	}

	protected synchronized Iterable<ChildProxyFactory<Z, D, T, E, ?, ? extends Exception>> children()
	{
		return this.children;
	}
	
	@Override
	public synchronized void addChild(ChildProxyFactory<Z, D, T, E, ?, ? extends Exception> child)
	{
		this.children.add(child);
	}

	@Override
	public synchronized void removeChild(ChildProxyFactory<Z, D, T, E, ?, ? extends Exception> child)
	{
		this.children.remove(child);
	}

	@Override
	public synchronized final void removeChildren()
	{
		this.children.clear();
	}

	/**
	 * Returns the underlying SQL object for the specified database.
	 * If the sql object does not exist (this might be the case if the database was newly activated), it will be created from the stored operation.
	 * Any recorded operations are also executed. If the object could not be created, or if any of the executed operations failed, then the specified database is deactivated.
	 * @param database a database descriptor.
	 * @return an underlying SQL object
	 */
	@Override
	public T get(D database)
	{
		synchronized (this.map)
		{
			T object = this.map.get(database);
			
			if (object == null)
			{
				try
				{
					object = this.create(database);
					
					this.replay(database, object);
					
					this.map.put(database, object);
				}
				catch (Throwable e)
				{
					if (!this.map.isEmpty() && this.cluster.deactivate(database, this.cluster.getStateManager()))
					{
						this.logger.log(Level.WARN, e, this.messages.proxyCreationFailed(this.cluster, database, this.getClass()));
					}
				}
			}
			
			return object;
		}
	}
	
	protected abstract T create(D database) throws TE;

	@Override
	public void record(Invoker<Z, D, T, ?, E> invoker)
	{
		// Record only the last invocation of a given set*(...) method
		synchronized (this.invokers)
		{
			this.invokers.remove(invoker);
			this.invokers.add(invoker);
		}
	}
	
	/**
	 * @throws E  
	 */
	@Override
	public void replay(D database, T object) throws E
	{
		synchronized (this.invokers)
		{
			for (Invoker<Z, D, T, ?, E> invoker: this.invokers)
			{
				this.logger.log(Level.TRACE, "Replaying {1}.{2} against database {0}", database, object.getClass().getName(), invoker);

				try
				{
					invoker.invoke(database, object);
				}
				catch (Throwable e)
				{
					this.exceptionFactory.createException(e);
				}
			}
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public final ExceptionFactory<E> getExceptionFactory()
	{
		return this.exceptionFactory;
	}
}
