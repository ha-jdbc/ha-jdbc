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
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.util.reflect.Methods;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Paul Ferraro
 */
public abstract class AbstractInvocationHandler<D, E> implements InvocationHandler, SQLProxy<D, E>
{
	protected Logger logger = LoggerFactory.getLogger(this.getClass());
	
	protected DatabaseCluster<D> databaseCluster;
	private Class<E> proxyClass;
	private Map<Database<D>, E> objectMap;
	private List<SQLProxy<D, ?>> childList = new LinkedList<SQLProxy<D, ?>>();
	private Map<Method, Invoker<D, E, ?>> invokerMap = new HashMap<Method, Invoker<D, E, ?>>();
	
	protected AbstractInvocationHandler(DatabaseCluster<D> databaseCluster, Class<E> proxyClass, Map<Database<D>, E> objectMap)
	{
		this.databaseCluster = databaseCluster;
		this.proxyClass = proxyClass;
		this.objectMap = objectMap;
	}
	
	/**
	 * @see java.lang.reflect.InvocationHandler#invoke(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@SuppressWarnings("unchecked")
	@Override
	public final Object invoke(Object object, Method method, Object[] parameters) throws Exception
	{
		if (!this.databaseCluster.isActive())
		{
			throw new SQLException(Messages.getMessage(Messages.CLUSTER_NOT_ACTIVE, this.databaseCluster));
		}
		
		E proxy = this.proxyClass.cast(object);
		
		InvocationStrategy strategy = this.getInvocationStrategy(proxy, method, parameters);
		Invoker invoker = this.getInvoker(proxy, method, parameters);
		
		Object result = strategy.invoke(this, invoker);
		
		this.record(method, invoker);
		
		this.postInvoke(proxy, method, parameters);
		
		return result;
	}
	
	@SuppressWarnings("nls")
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
			// Ignore - Java 1.4 or 1.5
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
		
		return new NonTransactionalDatabaseWriteInvocationStrategy<D, E, Object>();
	}
	
	protected Invoker<D, E, ?> getInvoker(E object, Method method, Object[] parameters) throws Exception
	{
		if (this.isSQLMethod(method))
		{
			List<Object> parameterList = new ArrayList<Object>(Arrays.asList(parameters));
			
			DatabaseCluster<D> cluster = this.getDatabaseCluster();
			
			long now = System.currentTimeMillis();
			
			if (cluster.isCurrentTimestampEvaluationEnabled())
			{
				parameterList.set(0, cluster.getDialect().evaluateCurrentTimestamp((String) parameterList.get(0), new java.sql.Timestamp(now)));
			}
			
			if (cluster.isCurrentDateEvaluationEnabled())
			{
				parameterList.set(0, cluster.getDialect().evaluateCurrentDate((String) parameterList.get(0), new java.sql.Date(now)));
			}
			
			if (cluster.isCurrentTimeEvaluationEnabled())
			{
				parameterList.set(0, cluster.getDialect().evaluateCurrentTime((String) parameterList.get(0), new java.sql.Time(now)));
			}
			
			if (cluster.isRandEvaluationEnabled())
			{
				parameterList.set(0, cluster.getDialect().evaluateRand((String) parameterList.get(0)));
			}
			
			return new SimpleInvoker(method, parameterList.toArray());
		}
		
		return new SimpleInvoker(method, parameters);
	}
	
	/**
	 * Indicates whether or not the specified method accepts a SQL string as its first parameter.
	 * @param method a method
	 * @return true, if the specified method accepts a SQL string as its first parameter, false otherwise.
	 */
	protected boolean isSQLMethod(Method method)
	{
		return false;
	}
	
	protected void postInvoke(E object, Method method, Object[] parameters) throws Exception
	{
		// Do nothing
	}
	
	/**
	 * @see net.sf.hajdbc.sql.SQLProxy#entry()
	 */
	@Override
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
	@Override
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
	@Override
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
	@Override
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
	@Override
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
	@Override
	public final E getObject(Database<D> database)
	{
		synchronized (this.objectMap)
		{
			E object = this.objectMap.get(database);
			
			if (object == null)
			{
				try
				{
					object = this.createObject(database);
					
					this.replay(database, object);
					
					this.objectMap.put(database, object);
				}
				catch (SQLException e)
				{
					if (!this.objectMap.isEmpty() && this.databaseCluster.deactivate(database, this.databaseCluster.getStateManager()))
					{
						this.logger.warn(Messages.getMessage(Messages.SQL_OBJECT_INIT_FAILED, this.getClass().getName(), database), e);
					}
				}
			}
			
			return object;
		}
	}
	
	protected abstract E createObject(Database<D> database) throws SQLException;

	protected void record(Method method, Invoker<D, E, ?> invoker)
	{
		// Record only the last invocation of a given set*(...) method
		if (this.isSetMethod(method) && (method.getTypeParameters().length == 1))
		{
			synchronized (this.invokerMap)
			{
				this.invokerMap.put(method, invoker);
			}
		}
	}
	
	@SuppressWarnings("nls")
	protected boolean isSetMethod(Method method)
	{
		return method.getName().startsWith("set") && (method.getTypeParameters() != null);
	}
	
	protected void replay(Database<D> database, E object) throws SQLException
	{
		synchronized (this.invokerMap)
		{
			for (Invoker<D, E, ?> invoker: this.invokerMap.values())
			{
				invoker.invoke(database, object);
			}
		}
	}
	
	/**
	 * @see net.sf.hajdbc.sql.SQLProxy#retain(java.util.Set)
	 */
	@Override
	public final void retain(Set<Database<D>> databaseSet)
	{
		synchronized (this.childList)
		{
			for (SQLProxy<D, ?> child: this.childList)
			{
				child.retain(databaseSet);
			}
		}
		
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
						this.close(database, object);
					}
					
					mapEntries.remove();
				}
			}
		}
	}

	protected abstract void close(Database<D> database, E object);
	
	/**
	 * @see net.sf.hajdbc.sql.SQLProxy#getDatabaseCluster()
	 */
	@Override
	public final DatabaseCluster<D> getDatabaseCluster()
	{
		return this.databaseCluster;
	}
	
	/**
	 * @see net.sf.hajdbc.sql.SQLProxy#handleFailure(net.sf.hajdbc.Database, java.sql.SQLException)
	 */
	@Override
	public void handleFailure(Database<D> database, SQLException exception) throws SQLException
	{
		Set<Database<D>> databaseSet = this.databaseCluster.getBalancer().all();
		
		// If cluster has only one database left, don't deactivate
		if (databaseSet.size() <= 1)
		{
			throw exception;
		}

		Map<Boolean, List<Database<D>>> aliveMap = this.databaseCluster.getAliveMap(databaseSet);
		
		this.detectClusterPanic(aliveMap);
		
		List<Database<D>> aliveList = aliveMap.get(true);
		
		// If all are dead, assume the worst and throw caught exception
		// If failed database is alive, then throw caught exception
		if (aliveList.isEmpty() || aliveList.contains(database))
		{
			throw exception;
		}
		
		// Otherwise deactivate failed database
		if (this.databaseCluster.deactivate(database, this.databaseCluster.getStateManager()))
		{
			this.logger.error(Messages.getMessage(Messages.DATABASE_DEACTIVATED, database, this), exception);
		}
	}
	
	/**
	 * @see net.sf.hajdbc.sql.SQLProxy#handleFailures(java.util.SortedMap)
	 */
	@Override
	public void handleFailures(SortedMap<Database<D>, SQLException> exceptionMap) throws SQLException
	{
		if (exceptionMap.size() == 1)
		{
			throw exceptionMap.get(exceptionMap.firstKey());
		}
		
		Map<Boolean, List<Database<D>>> aliveMap = this.databaseCluster.getAliveMap(exceptionMap.keySet());

		this.detectClusterPanic(aliveMap);
		
		List<Database<D>> aliveList = aliveMap.get(true);
		List<Database<D>> deadList = aliveMap.get(false);

		if (!aliveList.isEmpty())
		{
			for (Database<D> database: deadList)
			{
				if (this.databaseCluster.deactivate(database, this.databaseCluster.getStateManager()))
				{
					this.logger.error(Messages.getMessage(Messages.DATABASE_DEACTIVATED, database, this.databaseCluster), exceptionMap.get(database));
				}
			}
		}
		
		List<Database<D>> list = aliveList.isEmpty() ? deadList : aliveList;
		
		SQLException exception = exceptionMap.get(list.get(0));

		for (Database<D> database: list.subList(1, list.size()))
		{
			exception.setNextException(exceptionMap.get(database));
		}
		
		throw exception;
	}

	/**
	 * @see net.sf.hajdbc.sql.SQLProxy#handlePartialFailure(java.util.SortedMap, java.util.SortedMap)
	 */
	@Override
	public <R> SortedMap<Database<D>, R> handlePartialFailure(SortedMap<Database<D>, R> resultMap, SortedMap<Database<D>, SQLException> exceptionMap) throws SQLException
	{
		Map<Boolean, List<Database<D>>> aliveMap = this.databaseCluster.getAliveMap(exceptionMap.keySet());
		
		// Assume success databases are alive
		aliveMap.get(true).addAll(resultMap.keySet());
		
		this.detectClusterPanic(aliveMap);
		
		for (Map.Entry<Database<D>, SQLException> exceptionMapEntry: exceptionMap.entrySet())
		{
			Database<D> database = exceptionMapEntry.getKey();
			SQLException exception = exceptionMapEntry.getValue();
			
			if (this.databaseCluster.deactivate(database, this.databaseCluster.getStateManager()))
			{
				this.logger.error(Messages.getMessage(Messages.DATABASE_DEACTIVATED, database, this.databaseCluster), exception);
			}
		}
		
		return resultMap;
	}

	/**
	 * Detect cluster panic if all conditions are met:
	 * <ul>
	 * <li>We're in distributable mode</li>
	 * <li>We're the only group member</li>
	 * <li>All alive databases are local</li>
	 * <li>All dead databases are remote</li>
	 * </ul>
	 * @param aliveMap
	 * @throws SQLException
	 */
	protected void detectClusterPanic(Map<Boolean, List<Database<D>>> aliveMap) throws SQLException
	{
		if (this.databaseCluster.getStateManager().isMembershipEmpty())
		{
			List<Database<D>> aliveList = aliveMap.get(true);
			List<Database<D>> deadList = aliveMap.get(false);
			
			if (!aliveList.isEmpty() && !deadList.isEmpty() && sameProximity(aliveList, true) && sameProximity(deadList, false))
			{
				this.databaseCluster.stop();
				
				String message = Messages.getMessage(Messages.CLUSTER_PANIC_DETECTED, this.databaseCluster);
				
				this.logger.error(message);
				
				throw new SQLException(message);
			}
		}
	}
	
	private boolean sameProximity(List<Database<D>> databaseList, boolean local)
	{
		boolean same = true;
		
		for (Database<D> database: databaseList)
		{
			same &= (database.isLocal() == local);
		}
		
		return same;
	}
	
	protected class SimpleInvoker implements Invoker<D, E, Object>
	{
		private Method method;
		private Object[] parameters;
		
		public SimpleInvoker(Method method, Object[] parameters)
		{
			this.method = method;
			this.parameters = parameters;
		}
		
		/**
		 * @see net.sf.hajdbc.sql.Invoker#invoke(net.sf.hajdbc.Database, java.lang.Object)
		 */
		@Override
		public Object invoke(Database<D> database, E object) throws SQLException
		{
			return Methods.invoke(this.method, object, this.parameters);
		}
	}
}
