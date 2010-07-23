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
package net.sf.hajdbc.sql.xa;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
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
import net.sf.hajdbc.durability.Durability.Phase;
import net.sf.hajdbc.lock.LockManager;
import net.sf.hajdbc.sql.AbstractChildInvocationHandler;
import net.sf.hajdbc.sql.InvocationStrategyEnum;
import net.sf.hajdbc.sql.InvocationStrategy;
import net.sf.hajdbc.sql.Invoker;
import net.sf.hajdbc.sql.SQLProxy;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 *
 */
@SuppressWarnings("nls")
public class XAResourceInvocationHandler extends AbstractChildInvocationHandler<XADataSource, XADataSourceDatabase, XAConnection, SQLException, XAResource, XAException>
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
	
	private static final Map<Method, Durability.Phase> phaseMap = new IdentityHashMap<Method, Durability.Phase>();
	static
	{
		phaseMap.put(prepareMethod, Phase.PREPARE);
		phaseMap.put(commitMethod, Phase.COMMIT);
		phaseMap.put(rollbackMethod, Phase.ROLLBACK);
		phaseMap.put(forgetMethod, Phase.FORGET);
	}
	
	// Xids are global - so store in static variable
	private static final ConcurrentMap<Xid, Lock> lockMap = new ConcurrentHashMap<Xid, Lock>();
	
	/**
	 * @param connection
	 * @param proxy
	 * @param invoker
	 * @param objectMap
	 * @throws Exception
	 */
	protected XAResourceInvocationHandler(XAConnection connection, SQLProxy<XADataSource, XADataSourceDatabase, XAConnection, SQLException> proxy, Invoker<XADataSource, XADataSourceDatabase, XAConnection, XAResource, SQLException> invoker, Map<XADataSourceDatabase, XAResource> objectMap)
	{
		super(connection, proxy, invoker, XAResource.class, XAException.class, objectMap);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#getInvocationStrategy(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationStrategy getInvocationStrategy(XAResource resource, Method method, Object[] parameters) throws XAException
	{
		if (driverReadMethodSet.contains(method))
		{
			return InvocationStrategyEnum.INVOKE_ON_ANY;
		}
		
		if (databaseWriteMethodSet.contains(method))
		{
			return InvocationStrategyEnum.INVOKE_ON_ALL;
		}
		
		boolean start = method.equals(startMethod);
		boolean end = endTransactionMethodSet.contains(method);
		
		if (start || end || method.equals(prepareMethod) || intraTransactionMethodSet.contains(method))
		{
			final InvocationStrategy strategy = end ? InvocationStrategyEnum.END_TRANSACTION_INVOKE_ON_ALL : InvocationStrategyEnum.TRANSACTION_INVOKE_ON_ALL;
			
			Xid xid = (Xid) parameters[0];
			
			DatabaseCluster<XADataSource, XADataSourceDatabase> cluster = this.getDatabaseCluster();
			
			if (start)
			{
				final Lock lock = cluster.getLockManager().readLock(LockManager.GLOBAL);
				
				// Lock may already exist if we're resuming a suspended transaction
				if (lockMap.putIfAbsent(xid, lock) == null)
				{
					return new InvocationStrategy()
					{
						@Override
						public <Z, D extends Database<Z>, T, R, E extends Exception> SortedMap<D, R> invoke(SQLProxy<Z, D, T, E> proxy, Invoker<Z, D, T, R, E> invoker) throws E
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
			
			Durability.Phase phase = phaseMap.get(method);
			
			if (phase != null)
			{
				final InvocationStrategy durabilityStrategy = cluster.getDurability().getInvocationStrategy(strategy, phase, xid);
				
				if (endTransactionMethodSet.contains(method))
				{
					final Lock lock = lockMap.remove(xid);

					return new InvocationStrategy()
					{
						@Override
						public <Z, D extends Database<Z>, T, R, E extends Exception> SortedMap<D, R> invoke(SQLProxy<Z, D, T, E> proxy, Invoker<Z, D, T, R, E> invoker) throws E
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
	protected <R> Invoker<XADataSource, XADataSourceDatabase, XAResource, R, XAException> getInvoker(XAResource object, Method method, Object[] parameters) throws XAException
	{
		Invoker<XADataSource, XADataSourceDatabase, XAResource, R, XAException> invoker = super.getInvoker(object, method, parameters);
		
		Durability.Phase phase = phaseMap.get(method);
		
		if (method.equals(prepareMethod) || endTransactionMethodSet.contains(method))
		{
			return this.getDatabaseCluster().getDurability().getInvoker(invoker, phase, parameters[0], this.getExceptionFactory());
		}
		
		return invoker;
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#close(java.lang.Object, java.lang.Object)
	 */
	@Override
	protected void close(XAConnection connection, XAResource resource)
	{
		// Do nothing
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#isRecordable(java.lang.reflect.Method)
	 */
	@Override
	protected boolean isRecordable(Method method)
	{
		return databaseWriteMethodSet.contains(method);
	}
}
