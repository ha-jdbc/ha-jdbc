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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.ExceptionFactory;
import net.sf.hajdbc.ExceptionType;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggerFactory;

/**
 * 
 * @author Paul Ferraro
 */
public abstract class AbstractProxyFactory<Z, D extends Database<Z>, TE extends Exception, T, E extends Exception> implements ProxyFactory<Z, D, T, E>
{
	protected Logger logger = LoggerFactory.getLogger(this.getClass());
	
	private final Map<D, T> map;
	private final Map<ProxyFactory<Z, D, ?, ? extends Exception>, Object> children = new WeakHashMap<ProxyFactory<Z, D, ?, ? extends Exception>, Object>();
	private final Set<Invoker<Z, D, T, ?, E>> invokers = new HashSet<Invoker<Z, D, T, ?, E>>();
	private final ExceptionFactory<E> exceptionFactory;
	
	/**
	 * Constructs a new proxy to a set of objects
	 * @param exceptionClass the class for exceptions thrown by this object
	 * @param objectMap a map of database to sql object.
	 */
	protected AbstractProxyFactory(Map<D, T> map, Class<E> exceptionClass)
	{
		this.map = map;
		this.exceptionFactory = ExceptionType.valueOf(exceptionClass).getExceptionFactory();
	}

	/**
	 * @see net.sf.hajdbc.sql.SQLProxy#entries()
	 */
	@Override
	public Set<Map.Entry<D, T>> entries()
	{
		synchronized (this.map)
		{
			return this.map.entrySet();
		}
	}

	@Override
	public void addChild(ProxyFactory<Z, D, ?, ? extends Exception> child)
	{
		synchronized (this.children)
		{
			this.children.put(child, null);
		}
	}

	@Override
	public void removeChild(ProxyFactory<Z, D, ?, ? extends Exception> child)
	{
		synchronized (this.children)
		{
			this.children.remove(child);
		}
	}

	@Override
	public final void removeChildren()
	{
		synchronized (this.children)
		{
			this.children.clear();
		}
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
					DatabaseCluster<Z, D> cluster = this.getRoot().getDatabaseCluster();
					
					if (!this.map.isEmpty() && cluster.deactivate(database, cluster.getStateManager()))
					{
						this.logger.log(Level.WARN, e, Messages.SQL_OBJECT_INIT_FAILED.getMessage(), this.getClass().getName(), database);
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
	 * @see net.sf.hajdbc.sql.SQLProxy#retain(java.util.Set)
	 */
	@Override
	public final void retain(Set<D> databaseSet)
	{
		synchronized (this.children)
		{
			for (ProxyFactory<Z, D, ?, ? extends Exception> child: this.children.keySet())
			{
				child.retain(databaseSet);
			}
		}
		
		synchronized (this.map)
		{
			Iterator<Map.Entry<D, T>> mapEntries = this.map.entrySet().iterator();
			
			while (mapEntries.hasNext())
			{
				Map.Entry<D, T> mapEntry = mapEntries.next();
				
				D database = mapEntry.getKey();
				
				if (!databaseSet.contains(database))
				{
					T object = mapEntry.getValue();
					
					if (object != null)
					{
						this.close(database, object);
					}
					
					mapEntries.remove();
				}
			}
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.SQLProxy#getExceptionFactory()
	 */
	@Override
	public final ExceptionFactory<E> getExceptionFactory()
	{
		return this.exceptionFactory;
	}
}
