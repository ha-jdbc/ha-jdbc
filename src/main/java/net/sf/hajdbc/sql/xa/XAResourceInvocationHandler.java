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
package net.sf.hajdbc.sql.xa;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;

import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.durability.Durability;
import net.sf.hajdbc.durability.DurabilityPhaseRegistryBuilder;
import net.sf.hajdbc.invocation.InvocationStrategies;
import net.sf.hajdbc.invocation.InvocationStrategy;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.sql.ChildInvocationHandler;
import net.sf.hajdbc.sql.ProxyFactory;
import net.sf.hajdbc.util.StaticRegistry;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 *
 */
@SuppressWarnings("nls")
public class XAResourceInvocationHandler extends ChildInvocationHandler<XADataSource, XADataSourceDatabase, XAConnection, SQLException, XAResource, XAException, XAResourceProxyFactory>
{
	private static final Set<Method> driverReadMethodSet = Methods.findMethods(XAResource.class, "getTransactionTimeout", "isSameRM");
	private static final Set<Method> databaseWriteMethodSet = Methods.findMethods(XAResource.class, "setTransactionTimeout");
	private static final Set<Method> intraTransactionMethodSet = Methods.findMethods(XAResource.class, "prepare", "end", "recover");
	private static final Method prepareMethod = Methods.getMethod(XAResource.class, "prepare", Xid.class);
	private static final Method startMethod = Methods.getMethod(XAResource.class, "start", Xid.class, Integer.TYPE);
	private static final Method commitMethod = Methods.getMethod(XAResource.class, "commit", Xid.class, Boolean.TYPE);
	private static final Method rollbackMethod = Methods.getMethod(XAResource.class, "rollback", Xid.class);
	private static final Method forgetMethod = Methods.getMethod(XAResource.class, "forget", Xid.class);
	private static final Set<Method> endTransactionMethodSet = new HashSet<Method>(Arrays.asList(commitMethod, rollbackMethod, forgetMethod));

	private static final StaticRegistry<Method, Durability.Phase> phaseRegistry = new DurabilityPhaseRegistryBuilder().phase(Durability.Phase.PREPARE, prepareMethod).phase(Durability.Phase.COMMIT, commitMethod).phase(Durability.Phase.ROLLBACK, rollbackMethod).phase(Durability.Phase.FORGET, forgetMethod).build();
	
	// Xids are global - so store in static variable
	private static final ConcurrentMap<Xid, Lock> lockMap = new ConcurrentHashMap<Xid, Lock>();

	public XAResourceInvocationHandler(XAResourceProxyFactory proxyFactory)
	{
		super(XAResource.class, proxyFactory, null);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#getInvocationStrategy(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationStrategy getInvocationStrategy(XAResource resource, Method method, Object... parameters) throws XAException
	{
		if (driverReadMethodSet.contains(method))
		{
			return InvocationStrategies.INVOKE_ON_ANY;
		}
		
		if (databaseWriteMethodSet.contains(method))
		{
			return InvocationStrategies.INVOKE_ON_ALL;
		}
		
		boolean start = method.equals(startMethod);
		boolean end = endTransactionMethodSet.contains(method);
		
		if (start || end || intraTransactionMethodSet.contains(method))
		{
			final InvocationStrategy strategy = end ? InvocationStrategies.END_TRANSACTION_INVOKE_ON_ALL : InvocationStrategies.TRANSACTION_INVOKE_ON_ALL;
			
			Xid xid = (Xid) parameters[0];
			
			DatabaseCluster<XADataSource, XADataSourceDatabase> cluster = this.getProxyFactory().getDatabaseCluster();
			
			if (start)
			{
				final Lock lock = cluster.getLockManager().readLock(null);
				
				// Lock may already exist if we're resuming a suspended transaction
				if (lockMap.putIfAbsent(xid, lock) == null)
				{
					return new InvocationStrategy()
					{
						@Override
						public <Z, D extends Database<Z>, T, R, E extends Exception> SortedMap<D, R> invoke(ProxyFactory<Z, D, T, E> proxy, Invoker<Z, D, T, R, E> invoker) throws E
						{
							lock.lock();
							
							try
							{
								return strategy.invoke(proxy, invoker);
							}
							catch (Exception e)
							{
								lock.unlock();

								throw proxy.getExceptionFactory().createException(e);
							}
						}
					};
				}
			}
			
			Durability.Phase phase = phaseRegistry.get(method);
			if (phase != null)
			{
				final InvocationStrategy durabilityStrategy = cluster.getDurability().getInvocationStrategy(strategy, phase, xid);
				
				if (endTransactionMethodSet.contains(method))
				{
					final Lock lock = lockMap.remove(xid);

					return new InvocationStrategy()
					{
						@Override
						public <Z, D extends Database<Z>, T, R, E extends Exception> SortedMap<D, R> invoke(ProxyFactory<Z, D, T, E> proxy, Invoker<Z, D, T, R, E> invoker) throws E
						{
							try
							{
								return durabilityStrategy.invoke(proxy, invoker);
							}
							finally
							{
								if (lock != null)
								{
									lock.unlock();
								}
							}
						}
					};
				}
				
				return durabilityStrategy;
			}
			
			return strategy;
		}
		
		return super.getInvocationStrategy(resource, method, parameters);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#getInvoker(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected <R> Invoker<XADataSource, XADataSourceDatabase, XAResource, R, XAException> getInvoker(XAResource object, Method method, Object... parameters) throws XAException
	{
		Invoker<XADataSource, XADataSourceDatabase, XAResource, R, XAException> invoker = super.getInvoker(object, method, parameters);
		
		Durability.Phase phase = phaseRegistry.get(method);
		if (phase != null)
		{
			return this.getProxyFactory().getDatabaseCluster().getDurability().getInvoker(invoker, phase, parameters[0], this.getProxyFactory().getExceptionFactory());
		}
		
		return invoker;
	}

	@Override
	protected <R> void postInvoke(Invoker<XADataSource, XADataSourceDatabase, XAResource, R, XAException> invoker, XAResource proxy, Method method, Object... parameters)
	{
		if (databaseWriteMethodSet.contains(method))
		{
			this.getProxyFactory().record(invoker);
		}
	}
}
