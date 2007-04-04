/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2007 Paul Ferraro
 * 
 * This library is free software; you can redistribute it and/or modify it 
 * under the terms of the GNU Lesser General Public License as published by the 
 * Free Software Foundation; either version 2.1 of the License, or (at your 
 * option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License 
 * for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation, 
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 * 
 * Contact: ferraro@users.sourceforge.net
 */
package net.sf.hajdbc.sql;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.util.SQLExceptionFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Paul Ferraro
 *
 */
public abstract class AbstractInvocationHandler<D, P, E> implements InvocationHandler, SQLProxy<D, E>
{
	protected Logger logger = LoggerFactory.getLogger(this.getClass());
	
	private DatabaseCluster<D> databaseCluster;
	private P parentObject;
	private SQLProxy<D, P> parentProxy;
	private Invoker<D, P, E> parentInvoker;
	private Map<Database<D>, E> objectMap;
	private Set<Invoker<D, E, ?>> invokerSet = new LinkedHashSet<Invoker<D, E, ?>>();
	private List<SQLProxy<D, ?>> childList = new LinkedList<SQLProxy<D, ?>>();
	
	protected AbstractInvocationHandler(DatabaseCluster<D> databaseCluster, Map<Database<D>, E> objectMap)
	{
		this.databaseCluster = databaseCluster;
		this.objectMap = objectMap;
	}

	protected AbstractInvocationHandler(P object, SQLProxy<D, P> proxy, Invoker<D, P, E> invoker, Map<Database<D>, E> objectMap) throws Exception
	{
		this(proxy.getDatabaseCluster(), objectMap);
		
		this.parentObject = object;
		this.parentProxy = proxy;
		this.parentInvoker = invoker;
		this.parentProxy.addChild(this);
	}
	
	/**
	 * @see java.lang.reflect.InvocationHandler#invoke(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@SuppressWarnings("unchecked")
	public final Object invoke(Object object, Method method, Object[] parameters) throws Exception
	{
		E proxy = (E) object;
		
		InvocationStrategy strategy = this.getInvocationStrategy(proxy, method, parameters);
		Invoker invoker = this.getInvoker(proxy, method, parameters);
		
		Object result = strategy.invoke(this, invoker);
		
		this.postInvoke(proxy, method, parameters);
		
		return result;
	}
	
	protected InvocationStrategy<D, E, ?> getInvocationStrategy(final E object, Method method, final Object[] parameters) throws Exception
	{
		Class<?> objectClass = method.getDeclaringClass();
		
		try
		{
			// Most Java 1.6 sql classes implement java.sql.Wrapper
			if (method.equals(objectClass.getMethod("isWrapperFor", Class.class)) || method.equals(objectClass.getMethod("unwrap", Class.class)))
			{
				return new DriverReadInvocationStrategy<D, E, Object>();
			}
		}
		catch (NoSuchMethodException e)
		{
			// Ignore
		}
		
		if (method.equals(Object.class.getMethod("equals", Object.class)))
		{
			return new InvocationStrategy<D, E, Boolean>()
			{
				public Boolean invoke(SQLProxy<D, E> proxy, Invoker<D, E, Boolean> invoker) throws Exception
				{
					return object == parameters[0];
				}				
			};
		}
		
		if (method.equals(Object.class.getMethod("hashCode")) || method.equals(Object.class.getMethod("toString")))
		{
			return new DriverReadInvocationStrategy<D, E, Object>();
		}
		
		return new DatabaseWriteInvocationStrategy<D, E, Object>(null);
	}
	
	protected Invoker<D, E, ?> getInvoker(E object, Method method, Object[] parameters) throws Exception
	{
		return new DynamicInvoker(method, parameters);
	}
	
	protected void postInvoke(E object, Method method, Object[] parameters) throws Exception
	{
		// Do nothing
	}
	
	/**
	 * @see net.sf.hajdbc.sql.SQLProxy#entry()
	 */
	public Map.Entry<Database<D>, E> entry()
	{
		synchronized (this.objectMap)
		{
			return this.objectMap.entrySet().iterator().next();
		}
	}

	/**
	 * @see net.sf.hajdbc.sql.SQLProxy#entries()
	 */
	public Set<Map.Entry<Database<D>, E>> entries()
	{
		synchronized (this.objectMap)
		{
			return this.objectMap.entrySet();
		}
	}

	/**
	 * @see net.sf.hajdbc.sql.SQLProxy#addChild(net.sf.hajdbc.sql.SQLProxy)
	 */
	public final void addChild(SQLProxy<D, ?> child)
	{
		synchronized (this.childList)
		{
			this.childList.add(child);
		}
	}
	
	/**
	 * @see net.sf.hajdbc.sql.SQLProxy#removeChildren()
	 */
	public final void removeChildren()
	{
		synchronized (this.childList)
		{
			this.childList.clear();
		}
	}
	
	/**
	 * @see net.sf.hajdbc.sql.SQLProxy#removeChild(net.sf.hajdbc.sql.SQLProxy)
	 */
	public final void removeChild(SQLProxy<D, ?> child)
	{
		synchronized (this.childList)
		{
			child.removeChildren();
			
			this.childList.remove(child);
		}
	}
	
	/**
	 * Returns the underlying SQL object for the specified database.
	 * If the sql object does not exist (this might be the case if the database was newly activated), it will be created from the stored operation.
	 * Any recorded operations are also executed. If the object could not be created, or if any of the executed operations failed, then the specified database is deactivated.
	 * @param database a database descriptor.
	 * @return an underlying SQL object
	 */
	@SuppressWarnings("unchecked")
	public final E getObject(Database<D> database)
	{
		synchronized (this.objectMap)
		{
			E object = this.objectMap.get(database);
			
			if (object == null)
			{
				try
				{
					if (this.parentProxy == null)
					{
						throw new java.sql.SQLException();
					}
					
					P parentObject = this.parentProxy.getObject(database);
					
					if (parentObject == null)
					{
						throw new java.sql.SQLException();
					}
					
					object = this.parentInvoker.invoke(database, parentObject);
					
					synchronized (this.invokerSet)
					{
						for (Invoker<D, E, ?> invoker: this.invokerSet)
						{
							invoker.invoke(database, object);
						}
					}
					
					this.objectMap.put(database, object);
				}
				catch (Exception e)
				{
					if (this.databaseCluster.deactivate(database))
					{
						this.logger.warn(Messages.getMessage(Messages.SQL_OBJECT_INIT_FAILED, this.getClass().getName(), database), e);
					}
				}
			}
			
			return object;
		}
	}
	
	/**
	 * Records an operation.
	 * @param operation a database operation
	 */
	public final void record(Invoker<D, E, ?> invoker)
	{
		synchronized (this.invokerSet)
		{
			this.invokerSet.add(invoker);
		}
	}
	
	/**
	 * @see net.sf.hajdbc.sql.SQLProxy#retain(java.util.Set)
	 */
	public final void retain(Set<Database<D>> databaseSet)
	{
		synchronized (this.childList)
		{
			for (SQLProxy<D, ?> child: this.childList)
			{
				child.retain(databaseSet);
			}
		}
		
		if (this.parentProxy == null) return;
		
		synchronized (this.objectMap)
		{
			Iterator<Map.Entry<Database<D>, E>> mapEntries = this.objectMap.entrySet().iterator();
			
			while (mapEntries.hasNext())
			{
				Map.Entry<Database<D>, E> mapEntry = mapEntries.next();
				
				Database<D> database = mapEntry.getKey();
				
				if (!databaseSet.contains(database))
				{
					E object = mapEntry.getValue();
					
					if (object != null)
					{
						P parent = this.parentProxy.getObject(database);
						
						try
						{
							this.close(parent, object);
						}
						catch (SQLException e)
						{
							this.logger.info(e.getMessage(), e);
						}
					}
					
					mapEntries.remove();
				}
			}
		}
	}
	
	protected abstract void close(P parent, E object) throws SQLException;
	
	/**
	 * @see net.sf.hajdbc.sql.SQLProxy#getRoot()
	 */
	public final SQLProxy<D, ?> getRoot()
	{
		return (this.parentProxy == null) ? this : this.parentProxy.getRoot();
	}
	
	protected P getParent()
	{
		return this.parentObject;
	}
	
	protected SQLProxy<D, P> getParentProxy()
	{
		return this.parentProxy;
	}
	
	/**
	 * Returns the database cluster to which this proxy is associated.
	 * @return a database cluster
	 */
	public final DatabaseCluster<D> getDatabaseCluster()
	{
		return this.databaseCluster;
	}
	
	/**
	 * @param exceptionMap
	 * @throws java.sql.SQLException
	 */
	@SuppressWarnings("unused")
	public void handleFailures(SortedMap<Database<D>, SQLException> exceptionMap) throws SQLException
	{
		for (Map.Entry<Database<D>, java.sql.SQLException> exceptionMapEntry: exceptionMap.entrySet())
		{
			Database<D> database = exceptionMapEntry.getKey();
			java.sql.SQLException exception = exceptionMapEntry.getValue();
			
			if (this.databaseCluster.deactivate(database))
			{
				this.logger.error(Messages.getMessage(Messages.DATABASE_DEACTIVATED, database, this.databaseCluster), exception);
			}
		}
	}
	
	protected class DynamicInvoker implements Invoker<D, E, Object>
	{
		private Method method;
		private Object[] parameters;
		
		public DynamicInvoker(Method method, Object[] parameters)
		{
			this.method = method;
			this.parameters = parameters;
		}
		
		/**
		 * @see net.sf.hajdbc.sql.Invoker#invoke(net.sf.hajdbc.Database, java.lang.Object)
		 */
		public Object invoke(Database<D> database, E object) throws SQLException
		{
			try
			{
				return this.method.invoke(object, this.parameters);
			}
			catch (IllegalAccessException e)
			{
				throw new IllegalStateException(e);
			}
			catch (InvocationTargetException e)
			{
				Throwable target = e.getTargetException();
				
				if (SQLException.class.isInstance(target))
				{
					throw SQLException.class.cast(target);
				}
				else if (RuntimeException.class.isInstance(target))
				{
					throw RuntimeException.class.cast(target);
				}
				else if (Error.class.isInstance(target))
				{
					throw Error.class.cast(target);
				}
				
				throw SQLExceptionFactory.createSQLException(target);
			}
		}

		public Method getMethod()
		{
			return this.method;
		}
		
		public Object[] getParameters()
		{
			return this.parameters;
		}
		
		/**
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object object)
		{
			return (object != null) && DynamicInvoker.class.isInstance(object) && DynamicInvoker.class.cast(object).method.equals(this.method);
		}

		/**
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode()
		{
			return this.method.hashCode();
		}
	}
}
