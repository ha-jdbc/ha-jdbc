/*
 * HA-JDBC: High-Availability JDBC
 * Copyright 2004-2009 Paul Ferraro
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggerFactory;
import net.sf.hajdbc.util.reflect.Methods;

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
	private static final Method isWrapperForMethod = Methods.findMethod("java.sql.Wrapper", "isWrapperFor", Class.class);
	private static final Method unwrapMethod = Methods.findMethod("java.sql.Wrapper", "unwrap", Class.class);
	
	protected Logger logger = LoggerFactory.getLogger(this.getClass());
	
	protected DatabaseCluster<Z, D> cluster;
	private Class<T> proxyClass;
	private Map<D, T> objectMap;
	private Map<SQLProxy<Z, D, ?, ?>, Void> childMap = new WeakHashMap<SQLProxy<Z, D, ?, ?>, Void>();
	private Map<Method, Invoker<Z, D, T, ?, E>> invokerMap = new HashMap<Method, Invoker<Z, D, T, ?, E>>();
	
	/**
	 * @param cluster the database cluster
	 * @param proxyClass the interface being proxied
	 * @param objectMap a map of database to sql object.
	 */
	protected AbstractInvocationHandler(DatabaseCluster<Z, D> cluster, Class<T> proxyClass, Map<D, T> objectMap)
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
	public final Object invoke(Object object, Method method, Object[] parameters) throws Throwable
	{
//		if (method.equals(toStringMethod)) return "";
		
		if (!this.cluster.isActive())
		{
			throw new SQLException(Messages.CLUSTER_NOT_ACTIVE.getMessage(this.cluster));
		}
		
		T proxy = this.proxyClass.cast(object);
		
		InvocationStrategy strategy = this.getInvocationStrategy(proxy, method, parameters);
		Invoker invoker = this.getInvoker(proxy, method, parameters);
		this.logger.log(Level.TRACE, "Invoking "+method.getName()+" using " + strategy.getClass().getName());
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
	@SuppressWarnings("unused")
	protected InvocationStrategy<Z, D, T, ?, E> getInvocationStrategy(final T object, Method method, final Object[] parameters) throws E
	{
		// Most Java 1.6 sql classes implement java.sql.Wrapper
		if (((isWrapperForMethod != null) && method.equals(isWrapperForMethod)) || ((unwrapMethod != null) && method.equals(unwrapMethod)))
		{
			return new DriverReadInvocationStrategy<Z, D, T, Object, E>();
		}
		
		if (method.equals(equalsMethod))
		{
			return new InvocationStrategy<Z, D, T, Boolean, E>()
			{
				public Boolean invoke(SQLProxy<Z, D, T, E> proxy, Invoker<Z, D, T, Boolean, E> invoker)
				{
					return object == parameters[0];
				}				
			};
		}
		
		if (method.equals(hashCodeMethod) || method.equals(toStringMethod))
		{
			return new DriverReadInvocationStrategy<Z, D, T, Object, E>();
		}
		
		return new DatabaseWriteInvocationStrategy<Z, D, T, Object, E>(this.cluster.getNonTransactionalExecutor());
	}
	
	/**
	 * Return the appropriate invoker for the specified method.
	 * @param object
	 * @param method
	 * @param parameters
	 * @return an invoker
	 * @throws Exception
	 */
	@SuppressWarnings("unused")
	protected Invoker<Z, D, T, ?, E> getInvoker(T object, Method method, Object[] parameters) throws E
	{
		if (this.isSQLMethod(method))
		{
			List<Object> parameterList = new ArrayList<Object>(Arrays.asList(parameters));
			
			long now = System.currentTimeMillis();
			
			if (this.cluster.isCurrentTimestampEvaluationEnabled())
			{
				parameterList.set(0, this.cluster.getDialect().evaluateCurrentTimestamp((String) parameterList.get(0), new java.sql.Timestamp(now)));
			}
			
			if (this.cluster.isCurrentDateEvaluationEnabled())
			{
				parameterList.set(0, this.cluster.getDialect().evaluateCurrentDate((String) parameterList.get(0), new java.sql.Date(now)));
			}
			
			if (this.cluster.isCurrentTimeEvaluationEnabled())
			{
				parameterList.set(0, this.cluster.getDialect().evaluateCurrentTime((String) parameterList.get(0), new java.sql.Time(now)));
			}
			
			if (this.cluster.isRandEvaluationEnabled())
			{
				parameterList.set(0, this.cluster.getDialect().evaluateRand((String) parameterList.get(0)));
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
	public final void addChild(SQLProxy<Z, D, ?, ?> child)
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
	public final void removeChild(SQLProxy<Z, D, ?, ?> child)
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
					if (!this.objectMap.isEmpty() && this.cluster.deactivate(database, this.cluster.getStateManager()))
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
		System.out.println("replay");
		synchronized (this.invokerMap)
		{
			for (Invoker<Z, D, T, ?, E> invoker: this.invokerMap.values())
			{
				System.out.println("replaying: " + invoker);
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
			for (SQLProxy<Z, D, ?, ?> child: this.childMap.keySet())
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

	protected abstract void close(D database, T object);
	
	/**
	 * @see net.sf.hajdbc.sql.SQLProxy#getDatabaseCluster()
	 */
	@Override
	public final DatabaseCluster<Z, D> getDatabaseCluster()
	{
		return this.cluster;
	}
	
	@SuppressWarnings("unchecked")
	protected <A> SQLProxy<Z, D, A, E> getInvocationHandler(A proxy)
	{
		return (SQLProxy) Proxy.getInvocationHandler(proxy);
	}
	
	protected class SimpleInvoker implements Invoker<Z, D, T, Object, E>
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
		 * @see net.sf.hajdbc.sql.Invoker#invoke(net.sf.hajdbc.Database, java.lang.Object)
		 */
		@Override
		public Object invoke(D database, T object) throws E
		{
			return Methods.invoke(this.method, AbstractInvocationHandler.this.getExceptionFactory(), object, this.parameters);
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
}
