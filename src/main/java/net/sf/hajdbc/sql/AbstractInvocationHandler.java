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

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.sql.Wrapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.ExceptionFactory;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.invocation.AllResultsCollector;
import net.sf.hajdbc.invocation.InvocationStrategies;
import net.sf.hajdbc.invocation.InvocationStrategy;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.invocation.SimpleInvoker;
import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggerFactory;
import net.sf.hajdbc.sql.serial.SerialLocatorFactories;
import net.sf.hajdbc.sql.serial.SerialLocatorFactory;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * 
 * @author Paul Ferraro
 */
public class AbstractInvocationHandler<Z, D extends Database<Z>, T, E extends Exception, F extends ProxyFactory<Z, D, T, E>> implements InvocationHandler<Z, D, T, E, F>
{
	private static final Method equalsMethod = Methods.getMethod(Object.class, "equals", Object.class);
	private static final Method hashCodeMethod = Methods.getMethod(Object.class, "hashCode");
	private static final Method toStringMethod = Methods.getMethod(Object.class, "toString");
	private static final Set<Method> wrapperMethods = Methods.findMethods(Wrapper.class, "isWrapperFor", "unwrap");
	
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());
	private final Class<T> proxyClass;
	private final F proxyFactory;
	
	protected AbstractInvocationHandler(Class<T> targetClass, F proxyFactory)
	{
		this.proxyClass = targetClass;
		this.proxyFactory = proxyFactory;
	}
	
	@Override
	public F getProxyFactory()
	{
		return this.proxyFactory;
	}
	
	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable
	{
		DatabaseCluster<Z, D> cluster = this.proxyFactory.getDatabaseCluster();
		
		if (!cluster.isActive())
		{
			throw new SQLException(Messages.CLUSTER_NOT_ACTIVE.getMessage(cluster));
		}
		
		return this.invokeOnProxy(this.proxyClass.cast(proxy), method, args);
	}

	private <R> R invokeOnProxy(T proxy, Method method, Object... parameters) throws E
	{
		InvocationStrategy strategy = this.getInvocationStrategy(proxy, method, parameters);

		Invoker<Z, D, T, R, E> invoker = this.getInvoker(proxy, method, parameters);

		this.logger.log(Level.TRACE, "Invoking {0} using {1}", method, strategy);
		SortedMap<D, R> results = strategy.invoke(this.proxyFactory, invoker);

		this.postInvoke(invoker, proxy, method, parameters);
		
		@SuppressWarnings("unchecked")
		ProxyFactoryFactory<Z, D, T, E, R, ? extends Exception> factory = (ProxyFactoryFactory<Z, D, T, E, R, ? extends Exception>) this.getProxyFactoryFactory(proxy, method, parameters);
		InvocationResultFactory<Z, D, R> resultFactory = (factory != null) ? new ProxyInvocationResultFactory<Z, D, T, R, E>(factory, proxy, this.getProxyFactory(), invoker) : new SimpleInvocationResultFactory<Z, D, R>();
		
		return this.createResult(resultFactory, results);
	}
	
	/**
	 * @throws E 
	 */
	protected ProxyFactoryFactory<Z, D, T, E, ?, ? extends Exception> getProxyFactoryFactory(T object, Method method, Object... parameters) throws E
	{
		return null;
	}
	
	/**
	 * Returns the appropriate {@link InvocationStrategy} for the specified method.
	 * This implementation detects {@link java.sql.Wrapper} methods; and {@link Object#equals}, {@link Object#hashCode()}, and {@link Object#toString()}.
	 * Default invocation strategy is {@link AllResultsCollector}. 
	 * @param object the proxied object
	 * @param method the method to invoke
	 * @param parameters the method invocation parameters
	 * @return an invocation strategy
	 * @throws E
	 */
	protected InvocationStrategy getInvocationStrategy(T object, Method method, Object... parameters) throws E
	{
		if (equalsMethod.equals(method) || hashCodeMethod.equals(method) || toStringMethod.equals(method) || wrapperMethods.contains(method))
		{
			return InvocationStrategies.INVOKE_ON_ANY;
		}

		return InvocationStrategies.INVOKE_ON_ALL;
	}
	
	/**
	 * Return the appropriate invoker for the specified method.
	 * @param proxy
	 * @param method
	 * @param parameters
	 * @return an invoker
	 * @throws Exception
	 */
	protected <R> Invoker<Z, D, T, R, E> getInvoker(T proxy, Method method, Object... parameters) throws E
	{
		return this.getInvoker(method, parameters);
	}

	/**
	 * @throws E  
	 */
	private <R> Invoker<Z, D, T, R, E> getInvoker(Method method, Object... parameters) throws E
	{
		return new SimpleInvoker<Z, D, T, R, E>(method, parameters, this.proxyFactory.getExceptionFactory());
	}
	
	protected <R, X> Invoker<Z, D, T, R, E> getInvoker(Class<X> parameterClass, final int parameterIndex, T proxy, final Method method, final Object... parameters) throws E
	{
		if (parameterClass.equals(method.getParameterTypes()[parameterIndex]) && !parameterClass.isPrimitive())
		{
			X parameter = parameterClass.cast(parameters[parameterIndex]);
			
			if (parameter != null)
			{
				final ExceptionFactory<E> exceptionFactory = this.getProxyFactory().getExceptionFactory();
				
				// Handle proxy parameter
				if (Proxy.isProxyClass(parameter.getClass()) && (Proxy.getInvocationHandler(parameter) instanceof InvocationHandler))
				{
					final InvocationHandler<Z, D, X, E, ProxyFactory<Z, D, X, E>> handler = (InvocationHandler<Z, D, X, E, ProxyFactory<Z, D, X, E>>) Proxy.getInvocationHandler(parameter);
					
					return new Invoker<Z, D, T, R, E>()
					{
						@Override
						public R invoke(D database, T object) throws E
						{
							List<Object> parameterList = new ArrayList<Object>(Arrays.asList(parameters));
							
							parameterList.set(parameterIndex, handler.getProxyFactory().get(database));
							
							return Methods.<R, E>invoke(method, exceptionFactory, object, parameterList.toArray());
						}
					};
				}
				
				SerialLocatorFactory<X> factory = SerialLocatorFactories.find(parameterClass);
				if (factory != null)
				{
					try
					{
						// Create a serial form of the parameter, so it can be used by each database
						parameters[parameterIndex] = factory.createSerial(parameter);
					}
					catch (SQLException e)
					{
						throw exceptionFactory.createException(e);
					}
				}
			}
		}
		
		return this.getInvoker(method, parameters);
	}
	
	private <R> R createResult(InvocationResultFactory<Z, D, R> factory, SortedMap<D, R> resultMap) throws E
	{
		DatabaseCluster<Z, D> cluster = this.proxyFactory.getDatabaseCluster();
		
		if (resultMap.isEmpty())
		{
			throw this.proxyFactory.getExceptionFactory().createException(Messages.NO_ACTIVE_DATABASES.getMessage(cluster));
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

	protected <R> void postInvoke(Invoker<Z, D, T, R, E> invoker, T proxy, Method method, Object... parameters)
	{
		// Do nothing
	}
}
