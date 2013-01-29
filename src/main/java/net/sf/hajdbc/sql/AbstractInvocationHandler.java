/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.sql.Wrapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.WeakHashMap;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.ExceptionFactory;
import net.sf.hajdbc.ExceptionType;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.dialect.Dialect;
import net.sf.hajdbc.invocation.InvocationStrategy;
import net.sf.hajdbc.invocation.InvocationStrategyEnum;
import net.sf.hajdbc.invocation.InvokeOnAllInvocationStrategy;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggerFactory;
import net.sf.hajdbc.util.Objects;
import net.sf.hajdbc.util.reflect.Methods;
import net.sf.hajdbc.util.reflect.ProxyFactory;

/**
 * @author Paul Ferraro
 * @param <D> 
 * @param <T> 
 */
@SuppressWarnings("nls")
public abstract class AbstractInvocationHandler<Z, D extends Database<Z>, T, E extends Exception> implements InvocationHandler, SQLProxy<Z, D, T, E>
{
	private static final Method equalsMethod = Methods.getMethod(Object.class, "equals", Object.class);
	private static final Method hashCodeMethod = Methods.getMethod(Object.class, "hashCode");
	private static final Method toStringMethod = Methods.getMethod(Object.class, "toString");
	private static final Set<Method> wrapperMethods = Methods.findMethods(Wrapper.class, "isWrapperFor", "unwrap");
	
	protected Logger logger = LoggerFactory.getLogger(this.getClass());
	
	private final Class<T> proxyClass;
	private final Map<D, T> objectMap;
	private final Map<SQLProxy<Z, D, ?, ? extends Exception>, Void> childMap = new WeakHashMap<SQLProxy<Z, D, ?, ? extends Exception>, Void>();
	private final Map<Method, Invoker<Z, D, T, ?, E>> invokerMap = new HashMap<Method, Invoker<Z, D, T, ?, E>>();
	private final Class<E> exceptionClass;
	
	/**
	 * Constructs a new AbstractInvocationHandler
	 * @param proxyClass the interface being proxied
	 * @param exceptionClass the class for exceptions thrown by this object
	 * @param objectMap a map of database to sql object.
	 */
	protected AbstractInvocationHandler(Class<T> proxyClass, Class<E> exceptionClass, Map<D, T> objectMap)
	{
		this.proxyClass = proxyClass;
		this.exceptionClass = exceptionClass;
		this.objectMap = objectMap;
	}
	
	/**
	 * @see java.lang.reflect.InvocationHandler#invoke(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	public Object invoke(Object object, Method method, Object[] parameters) throws Throwable
	{
		DatabaseCluster<Z, D> cluster = this.getDatabaseCluster();
		
		if (!cluster.isActive())
		{
			throw new SQLException(Messages.CLUSTER_NOT_ACTIVE.getMessage(cluster));
		}
		
		return this.invokeOnProxy(this.proxyClass.cast(object), method, parameters);
	}

	private <R> R invokeOnProxy(T object, Method method, Object[] parameters) throws E
	{
		InvocationStrategy strategy = this.getInvocationStrategy(object, method, parameters);

		Invoker<Z, D, T, R, E> invoker = this.getInvoker(object, method, parameters);
		
		this.logger.log(Level.TRACE, "Invoking {0} using {1}", method, strategy.getClass().getName());
		
		SortedMap<D, R> results = strategy.invoke(this, invoker);
		
		this.record(invoker, method, parameters);
		
		this.postInvoke(object, method, parameters);
		
		@SuppressWarnings("unchecked")
		InvocationHandlerFactory<Z, D, T, R, E> handlerFactory = (InvocationHandlerFactory<Z, D, T, R, E>) this.getInvocationHandlerFactory(object, method, parameters);
		InvocationResultFactory<Z, D, R, E> resultFactory = (handlerFactory != null) ? new ProxyInvocationResultFactory<R>(handlerFactory, object, invoker) : new SimpleInvocationResultFactory<R>();
		
		return this.createResult(resultFactory, results);
	}
	
	protected InvocationHandlerFactory<Z, D, T, ?, E> getInvocationHandlerFactory(T object, Method method, Object[] parameters) throws E
	{
		return null;
	}
	
	/**
	 * Returns the appropriate {@link InvocationStrategy} for the specified method.
	 * This implementation detects {@link java.sql.Wrapper} methods; and {@link Object#equals}, {@link Object#hashCode()}, and {@link Object#toString()}.
	 * Default invocation strategy is {@link InvokeOnAllInvocationStrategy}. 
	 * @param object the proxied object
	 * @param method the method to invoke
	 * @param parameters the method invocation parameters
	 * @return an invocation strategy
	 * @throws Exception
	 */
	protected InvocationStrategy getInvocationStrategy(T object, Method method, Object[] parameters) throws E
	{
		if (method.equals(equalsMethod) || method.equals(hashCodeMethod) || method.equals(toStringMethod) || wrapperMethods.contains(method))
		{
			return InvocationStrategyEnum.INVOKE_ON_ANY;
		}

		return InvocationStrategyEnum.INVOKE_ON_ALL;
	}
	
	/**
	 * Return the appropriate invoker for the specified method.
	 * @param object
	 * @param method
	 * @param parameters
	 * @return an invoker
	 * @throws Exception
	 */
	protected <R> Invoker<Z, D, T, R, E> getInvoker(T object, Method method, Object[] parameters) throws E
	{
		if (this.isSQLMethod(method))
		{
			List<Object> parameterList = new ArrayList<Object>(Arrays.asList(parameters));
			
			long now = System.currentTimeMillis();
			
			DatabaseCluster<Z, D> cluster = this.getDatabaseCluster();
			Dialect dialect = cluster.getDialect();
			
			if (cluster.isCurrentTimestampEvaluationEnabled())
			{
				parameterList.set(0, dialect.evaluateCurrentTimestamp((String) parameterList.get(0), new java.sql.Timestamp(now)));
			}
			
			if (cluster.isCurrentDateEvaluationEnabled())
			{
				parameterList.set(0, dialect.evaluateCurrentDate((String) parameterList.get(0), new java.sql.Date(now)));
			}
			
			if (cluster.isCurrentTimeEvaluationEnabled())
			{
				parameterList.set(0, dialect.evaluateCurrentTime((String) parameterList.get(0), new java.sql.Time(now)));
			}
			
			if (cluster.isRandEvaluationEnabled())
			{
				parameterList.set(0, dialect.evaluateRand((String) parameterList.get(0)));
			}
			
			return new SimpleInvoker<R>(method, parameterList.toArray());
		}
		
		return new SimpleInvoker<R>(method, parameters);
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
	 * @see net.sf.hajdbc.sql.SQLProxy#entries()
	 */
	@Override
	public Set<Map.Entry<D, T>> entries()
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
	public final void addChild(SQLProxy<Z, D, ?, ? extends Exception> child)
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
	public final void removeChild(SQLProxy<Z, D, ?, ? extends Exception> child)
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
	public T getObject(D database)
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
				catch (Throwable e)
				{
					DatabaseCluster<Z, D> cluster = this.getDatabaseCluster();
					
					if (!this.objectMap.isEmpty() && cluster.deactivate(database, cluster.getStateManager()))
					{
						this.logger.log(Level.WARN, e, Messages.SQL_OBJECT_INIT_FAILED.getMessage(), this.getClass().getName(), database);
					}
				}
			}
			
			return object;
		}
	}
	
	protected abstract T createObject(D database) throws E;

	protected void record(Invoker<Z, D, T, ?, E> invoker, Method method, Object[] parameters)
	{
		// Record only the last invocation of a given set*(...) method
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
	
	protected void replay(D database, T object) throws E
	{
		synchronized (this.invokerMap)
		{
			for (Invoker<Z, D, T, ?, E> invoker: this.invokerMap.values())
			{
				this.logger.log(Level.TRACE, "Replaying against database {0}: {1}.{2}", database, object.getClass().getName(), invoker);

				invoker.invoke(database, object);
			}
		}
	}
	
	/**
	 * @see net.sf.hajdbc.sql.SQLProxy#retain(java.util.Set)
	 */
	@Override
	public final void retain(Set<D> databaseSet)
	{
		synchronized (this.childMap)
		{
			for (SQLProxy<Z, D, ?, ? extends Exception> child: this.childMap.keySet())
			{
				child.retain(databaseSet);
			}
		}
		
		synchronized (this.objectMap)
		{
			Iterator<Map.Entry<D, T>> mapEntries = this.objectMap.entrySet().iterator();
			
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

	private <R> R createResult(InvocationResultFactory<Z, D, R, E> factory, SortedMap<D, R> resultMap) throws E
	{
		DatabaseCluster<Z, D> cluster = this.getDatabaseCluster();
		
		if (resultMap.isEmpty())
		{
			throw this.getExceptionFactory().createException(Messages.NO_ACTIVE_DATABASES.getMessage(cluster));
		}
		
		Iterator<Map.Entry<D, R>> results = resultMap.entrySet().iterator();
		R primaryResult = results.next().getValue();
		
		while (results.hasNext())
		{
			Map.Entry<D, R> entry = results.next();
			R result = entry.getValue();
			
			if (factory.differs(primaryResult, result))
			{
				results.remove();
				D database = entry.getKey();
				
				if (cluster.deactivate(database, cluster.getStateManager()))
				{
					this.logger.log(Level.ERROR, Messages.DATABASE_INCONSISTENT.getMessage(), database, cluster, primaryResult, result);
				}
			}
		}
		
		return (primaryResult != null) ? factory.createResult(resultMap) : null;
	}

	protected abstract void close(D database, T object);
	
	@SuppressWarnings("unchecked")
	protected <A> SQLProxy<Z, D, A, E> getInvocationHandler(A proxy)
	{
		return (SQLProxy<Z, D, A, E>) Proxy.getInvocationHandler(proxy);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.SQLProxy#getExceptionFactory()
	 */
	@Override
	public final ExceptionFactory<E> getExceptionFactory()
	{
		return ExceptionType.getExceptionFactory(this.exceptionClass);
	}
	
	protected class SimpleInvoker<R> implements Invoker<Z, D, T, R, E>
	{
		private final Method method;
		private final Object[] parameters;
		
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
		 * @see net.sf.hajdbc.invocation.Invoker#invoke(net.sf.hajdbc.Database, java.lang.Object)
		 */
		@Override
		public R invoke(D database, T object) throws E
		{
			return Methods.<R, E>invoke(this.method, AbstractInvocationHandler.this.getExceptionFactory(), object, this.parameters);
		}

		/**
		 * {@inheritDoc}
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString()
		{
			return this.method.toString();
		}
	}
	
	class SimpleInvocationResultFactory<R> implements InvocationResultFactory<Z, D, R, E>
	{
		@Override
		public boolean differs(R primaryResult, R backupResult)
		{
			return !Objects.equals(primaryResult, backupResult);
		}
		
		@Override
		public R createResult(SortedMap<D, R> results)
		{
			return results.values().iterator().next();
		}
	}
	
	class ProxyInvocationResultFactory<R> implements InvocationResultFactory<Z, D, R, E>
	{
		private final InvocationHandlerFactory<Z, D, T, R, E> factory;
		private final T object;
		private final Invoker<Z, D, T, R, E> invoker;
		
		ProxyInvocationResultFactory(InvocationHandlerFactory<Z, D, T, R, E> factory, T object, Invoker<Z, D, T, R, E> invoker)
		{
			this.factory = factory;
			this.object = object;
			this.invoker = invoker;
		}
		
		@Override
		public boolean differs(R primaryResult, R backupResult)
		{
			return ((primaryResult != null) && (backupResult == null)) || ((primaryResult == null) && (backupResult != null));
		}

		@Override
		public R createResult(SortedMap<D, R> results) throws E
		{
			return ProxyFactory.createProxy(this.factory.getTargetClass(), this.factory.createInvocationHandler(this.object, AbstractInvocationHandler.this, this.invoker, results));
		}
	}
}
