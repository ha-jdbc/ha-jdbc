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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.WeakHashMap;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.util.SQLExceptionFactory;
import net.sf.hajdbc.util.reflect.Methods;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Paul Ferraro
 * @param <D> 
 * @param <T> 
 */
@SuppressWarnings("nls")
public abstract class AbstractInvocationHandler<D, T> implements InvocationHandler, SQLProxy<D, T>
{
	private static final Method equalsMethod = Methods.getMethod(Object.class, "equals", Object.class);
	private static final Method hashCodeMethod = Methods.getMethod(Object.class, "hashCode");
	private static final Method toStringMethod = Methods.getMethod(Object.class, "toString");
	/* JDBC 4.0 methods */
	private static final Method isWrapperForMethod = Methods.findMethod("java.sql.Wrapper", "isWrapperFor", Class.class);
	private static final Method unwrapMethod = Methods.findMethod("java.sql.Wrapper", "unwrap", Class.class);
	
	protected Logger logger = LoggerFactory.getLogger(this.getClass());
	
	protected DatabaseCluster<D> cluster;
	private Class<T> proxyClass;
	private Map<Database<D>, T> objectMap;
	private Map<SQLProxy<D, ?>, Void> childMap = new WeakHashMap<SQLProxy<D, ?>, Void>();
	private Map<Method, Invoker<D, T, ?>> invokerMap = new HashMap<Method, Invoker<D, T, ?>>();
	
	/**
	 * @param cluster the database cluster
	 * @param proxyClass the interface being proxied
	 * @param objectMap a map of database to sql object.
	 */
	protected AbstractInvocationHandler(DatabaseCluster<D> cluster, Class<T> proxyClass, Map<Database<D>, T> objectMap)
	{
		this.cluster = cluster;
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
		if (method.equals(toStringMethod)) return "";
		
		if (!this.cluster.isActive())
		{
			throw new SQLException(Messages.getMessage(Messages.CLUSTER_NOT_ACTIVE, this.cluster));
		}
		
		T proxy = this.proxyClass.cast(object);
		
		InvocationStrategy strategy = this.getInvocationStrategy(proxy, method, parameters);
		Invoker invoker = this.getInvoker(proxy, method, parameters);
		
		Object result = strategy.invoke(this, invoker);
		
		this.record(invoker, method, parameters);
		
		this.postInvoke(proxy, method, parameters);
		
		return result;
	}
	
	/**
	 * Returns the appropriate {@link InvocationStrategy} for the specified method.
	 * This implementation detects {@link java.sql.Wrapper} methods; and {@link Object#equals}, {@link Object#hashCode()}, and {@link Object#toString()}.
	 * Default invocation strategy is {@link DatabaseWriteInvocationStrategy}. 
	 * @param object the proxied object
	 * @param method the method to invoke
	 * @param parameters the method invocation parameters
	 * @return an invocation strategy
	 * @throws Exception
	 */
	protected InvocationStrategy<D, T, ?> getInvocationStrategy(final T object, Method method, final Object[] parameters) throws Exception
	{
		// Most Java 1.6 sql classes implement java.sql.Wrapper
		if (((isWrapperForMethod != null) && method.equals(isWrapperForMethod)) || ((unwrapMethod != null) && method.equals(unwrapMethod)))
		{
			return new DriverReadInvocationStrategy<D, T, Object>();
		}
		
		if (method.equals(equalsMethod))
		{
			return new InvocationStrategy<D, T, Boolean>()
			{
				public Boolean invoke(SQLProxy<D, T> proxy, Invoker<D, T, Boolean> invoker)
				{
					return object == parameters[0];
				}				
			};
		}
		
		if (method.equals(hashCodeMethod) || method.equals(toStringMethod))
		{
			return new DriverReadInvocationStrategy<D, T, Object>();
		}
		
		return new DatabaseWriteInvocationStrategy<D, T, Object>(this.cluster.getNonTransactionalExecutor());
	}
	
	/**
	 * Return the appropriate invoker for the specified method.
	 * @param object
	 * @param method
	 * @param parameters
	 * @return an invoker
	 * @throws Exception
	 */
	protected Invoker<D, T, ?> getInvoker(T object, Method method, Object[] parameters) throws Exception
	{
		if (this.isSQLMethod(method))
		{
			long now = System.currentTimeMillis();
			
			if (this.cluster.isCurrentTimestampEvaluationEnabled())
			{
				parameters[0] = this.cluster.getDialect().evaluateCurrentTimestamp((String) parameters[0], new java.sql.Timestamp(now));
			}
			
			if (this.cluster.isCurrentDateEvaluationEnabled())
			{
				parameters[0] = this.cluster.getDialect().evaluateCurrentDate((String) parameters[0], new java.sql.Date(now));
			}
			
			if (this.cluster.isCurrentTimeEvaluationEnabled())
			{
				parameters[0] = this.cluster.getDialect().evaluateCurrentTime((String) parameters[0], new java.sql.Time(now));
			}
			
			if (this.cluster.isRandEvaluationEnabled())
			{
				parameters[0] = this.cluster.getDialect().evaluateRand((String) parameters[0]);
			}
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
	
	/**
	 * Called after method is invoked.
	 * @param proxy the proxied object
	 * @param method the method that was just invoked
	 * @param parameters the parameters of the method that was just invoked
	 */
	protected void postInvoke(T proxy, Method method, Object[] parameters)
	{
		// Do nothing
	}
	
	/**
	 * @see net.sf.hajdbc.sql.SQLProxy#entry()
	 */
	@Override
	public Map.Entry<Database<D>, T> entry()
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
	public Set<Map.Entry<Database<D>, T>> entries()
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
		synchronized (this.childMap)
		{
			this.childMap.put(child, null);
		}
	}
	
	/**
	 * @see net.sf.hajdbc.sql.SQLProxy#removeChildren()
	 */
	@Override
	public final void removeChildren()
	{
		synchronized (this.childMap)
		{
			this.childMap.clear();
		}
	}
	
	/**
	 * @see net.sf.hajdbc.sql.SQLProxy#removeChild(net.sf.hajdbc.sql.SQLProxy)
	 */
	@Override
	public final void removeChild(SQLProxy<D, ?> child)
	{
		child.removeChildren();
		
		synchronized (this.childMap)
		{
			this.childMap.remove(child);
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
	public T getObject(Database<D> database)
	{
		synchronized (this.objectMap)
		{
			T object = this.objectMap.get(database);
			
			if (object == null)
			{
				try
				{
					object = this.createObject(database);
					
					this.replay(database, object);
					
					this.objectMap.put(database, object);
				}
				catch (Exception e)
				{
					if (!this.objectMap.isEmpty() && this.cluster.deactivate(database, this.cluster.getStateManager()))
					{
						this.logger.warn(Messages.getMessage(Messages.SQL_OBJECT_INIT_FAILED, this.getClass().getName(), database), e);
					}
				}
			}
			
			return object;
		}
	}
	
	protected abstract T createObject(Database<D> database) throws Exception;

	protected void record(Invoker<D, T, ?> invoker, Method method, Object[] parameters)
	{
		// Record only the last invocation of a given recordable method
		if (this.isRecordable(method))
		{
			synchronized (this.invokerMap)
			{
				this.invokerMap.put(method, invoker);
			}
		}
	}
	
	protected boolean isRecordable(Method method)
	{
		return false;
	}
	
	protected void replay(Database<D> database, T object) throws Exception
	{
		synchronized (this.invokerMap)
		{
			for (Invoker<D, T, ?> invoker: this.invokerMap.values())
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
		synchronized (this.childMap)
		{
			for (SQLProxy<D, ?> child: this.childMap.keySet())
			{
				child.retain(databaseSet);
			}
		}
		
		synchronized (this.objectMap)
		{
			Iterator<Map.Entry<Database<D>, T>> mapEntries = this.objectMap.entrySet().iterator();
			
			while (mapEntries.hasNext())
			{
				Map.Entry<Database<D>, T> mapEntry = mapEntries.next();
				
				Database<D> database = mapEntry.getKey();
				
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

	protected abstract void close(Database<D> database, T object);
	
	/**
	 * @see net.sf.hajdbc.sql.SQLProxy#getDatabaseCluster()
	 */
	@Override
	public final DatabaseCluster<D> getDatabaseCluster()
	{
		return this.cluster;
	}
	
	/**
	 * @see net.sf.hajdbc.sql.SQLProxy#handleFailure(net.sf.hajdbc.Database, java.lang.Exception)
	 */
	@Override
	public void handleFailure(Database<D> database, Exception exception) throws Exception
	{
		Set<Database<D>> databaseSet = this.cluster.getBalancer().all();
		
		// If cluster has only one database left, don't deactivate
		if (databaseSet.size() <= 1)
		{
			throw exception;
		}

		Map<Boolean, List<Database<D>>> aliveMap = this.cluster.getAliveMap(databaseSet);
		
		this.detectClusterPanic(aliveMap);
		
		List<Database<D>> aliveList = aliveMap.get(true);
		
		// If all are dead, assume the worst and throw caught exception
		// If failed database is alive, then throw caught exception
		if (aliveList.isEmpty() || aliveList.contains(database))
		{
			throw exception;
		}
		
		// Otherwise deactivate failed database
		if (this.cluster.deactivate(database, this.cluster.getStateManager()))
		{
			this.logger.error(Messages.getMessage(Messages.DATABASE_DEACTIVATED, database, this), exception);
		}
	}
	
	/**
	 * @see net.sf.hajdbc.sql.SQLProxy#handleFailures(java.util.SortedMap)
	 */
	@Override
	public void handleFailures(SortedMap<Database<D>, Exception> exceptionMap) throws Exception
	{
		if (exceptionMap.size() == 1)
		{
			throw exceptionMap.get(exceptionMap.firstKey());
		}
		
		Map<Boolean, List<Database<D>>> aliveMap = this.cluster.getAliveMap(exceptionMap.keySet());

		this.detectClusterPanic(aliveMap);
		
		List<Database<D>> aliveList = aliveMap.get(true);
		List<Database<D>> deadList = aliveMap.get(false);

		if (!aliveList.isEmpty())
		{
			for (Database<D> database: deadList)
			{
				if (this.cluster.deactivate(database, this.cluster.getStateManager()))
				{
					this.logger.error(Messages.getMessage(Messages.DATABASE_DEACTIVATED, database, this.cluster), exceptionMap.get(database));
				}
			}
		}
		
		List<Database<D>> list = aliveList.isEmpty() ? deadList : aliveList;
		
		SQLException exception = SQLExceptionFactory.createSQLException(exceptionMap.get(list.get(0)));

		for (Database<D> database: list.subList(1, list.size()))
		{
			exception.setNextException(SQLExceptionFactory.createSQLException(exceptionMap.get(database)));
		}
		
		throw exception;
	}
	
	/**
	 * @see net.sf.hajdbc.sql.SQLProxy#handlePartialFailure(java.util.SortedMap, java.util.SortedMap)
	 */
	@Override
	public <R> SortedMap<Database<D>, R> handlePartialFailure(SortedMap<Database<D>, R> resultMap, SortedMap<Database<D>, Exception> exceptionMap) throws Exception
	{
		Map<Boolean, List<Database<D>>> aliveMap = this.cluster.getAliveMap(exceptionMap.keySet());
		
		// Assume success databases are alive
		aliveMap.get(true).addAll(resultMap.keySet());
		
		this.detectClusterPanic(aliveMap);
		
		for (Map.Entry<Database<D>, Exception> exceptionMapEntry: exceptionMap.entrySet())
		{
			Database<D> database = exceptionMapEntry.getKey();
			Exception exception = exceptionMapEntry.getValue();
			
			if (this.cluster.deactivate(database, this.cluster.getStateManager()))
			{
				this.logger.error(Messages.getMessage(Messages.DATABASE_DEACTIVATED, database, this.cluster), exception);
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
	 * @throws Exception
	 */
	protected void detectClusterPanic(Map<Boolean, List<Database<D>>> aliveMap) throws Exception
	{
		if (this.cluster.getStateManager().isMembershipEmpty())
		{
			List<Database<D>> aliveList = aliveMap.get(true);
			List<Database<D>> deadList = aliveMap.get(false);
			
			if (!aliveList.isEmpty() && !deadList.isEmpty() && sameProximity(aliveList, true) && sameProximity(deadList, false))
			{
				this.cluster.stop();
				
				String message = Messages.getMessage(Messages.CLUSTER_PANIC_DETECTED, this.cluster);
				
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
	
	protected class SimpleInvoker implements Invoker<D, T, Object>
	{
		private Method method;
		private Object[] parameters;
		
		/**
		 * @param method
		 * @param parameters
		 */
		public SimpleInvoker(Method method, Object[] parameters)
		{
			this.method = method;
			this.parameters = parameters;
		}
		
		/**
		 * @see net.sf.hajdbc.sql.Invoker#invoke(net.sf.hajdbc.Database, java.lang.Object)
		 */
		@Override
		public Object invoke(Database<D> database, T object) throws Exception
		{
			return Methods.invoke(this.method, object, this.parameters);
		}
	}
}
