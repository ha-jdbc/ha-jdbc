/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2008 Paul Ferraro
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
package net.sf.hajdbc.sql.xa;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;

import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import net.sf.hajdbc.ExceptionFactory;
import net.sf.hajdbc.durability.Durability;
import net.sf.hajdbc.durability.TransactionIdentifier;
import net.sf.hajdbc.durability.Durability.Phase;
import net.sf.hajdbc.lock.LockManager;
import net.sf.hajdbc.sql.AbstractChildInvocationHandler;
import net.sf.hajdbc.sql.DatabaseWriteInvocationStrategy;
import net.sf.hajdbc.sql.DriverReadInvocationStrategy;
import net.sf.hajdbc.sql.InvocationStrategy;
import net.sf.hajdbc.sql.Invoker;
import net.sf.hajdbc.sql.SQLProxy;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 *
 */
@SuppressWarnings("nls")
public class XAResourceInvocationHandler extends AbstractChildInvocationHandler<XADataSource, XADataSourceDatabase, XAConnection, XAResource, XAException>
{
	private static final Set<Method> driverReadMethodSet = Methods.findMethods(XAResource.class, "getTransactionTimeout", "isSameRM");
	private static final Set<Method> databaseWriteMethodSet = Methods.findMethods(XAResource.class, "setTransactionTimeout");
	private static final Set<Method> intraTransactionMethodSet = Methods.findMethods(XAResource.class, "end", "recover");
	private static final Method prepareMethod = Methods.getMethod(XAResource.class, "prepare", Xid.class);
	private static final Method startMethod = Methods.getMethod(XAResource.class, "start", Xid.class, Integer.TYPE);
	private static final Method commitMethod = Methods.getMethod(XAResource.class, "commit", Xid.class, Boolean.TYPE);
	private static final Method rollbackMethod = Methods.getMethod(XAResource.class, "rollback", Xid.class);
	private static final Method forgetMethod = Methods.getMethod(XAResource.class, "forget", Xid.class);
	
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
		super(connection, proxy, invoker, XAResource.class, objectMap);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#getInvocationStrategy(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationStrategy<XADataSource, XADataSourceDatabase, XAResource, ?, XAException> getInvocationStrategy(XAResource resource, Method method, Object[] parameters) throws XAException
	{
		if (driverReadMethodSet.contains(method))
		{
			return new DriverReadInvocationStrategy<XADataSource, XADataSourceDatabase, XAResource, Object, XAException>();
		}
		
		if (databaseWriteMethodSet.contains(method))
		{
			return new DatabaseWriteInvocationStrategy<XADataSource, XADataSourceDatabase, XAResource, Object, XAException>(this.cluster.getNonTransactionalExecutor());
		}
		
		boolean start = method.equals(startMethod);
		boolean prepare = method.equals(prepareMethod);
		boolean commit = method.equals(commitMethod);
		boolean rollback = method.equals(rollbackMethod);
		boolean forget = method.equals(forgetMethod);
		
		if (start || prepare || commit || rollback || forget || intraTransactionMethodSet.contains(method))
		{
			final InvocationStrategy<XADataSource, XADataSourceDatabase, XAResource, Object, XAException> strategy = new DatabaseWriteInvocationStrategy<XADataSource, XADataSourceDatabase, XAResource, Object, XAException>(this.cluster.getTransactionalExecutor());
			
			Xid xid = (Xid) parameters[0];
			
			if (start)
			{
				final Lock lock = this.cluster.getLockManager().readLock(LockManager.GLOBAL);
				
				// Lock may already exist if we're resuming a suspended transaction
				if (lockMap.putIfAbsent(xid, lock) == null)
				{
					return new InvocationStrategy<XADataSource, XADataSourceDatabase, XAResource, Object, XAException>()
					{
						@Override
						public Object invoke(SQLProxy<XADataSource, XADataSourceDatabase, XAResource, XAException> proxy, Invoker<XADataSource, XADataSourceDatabase, XAResource, Object, XAException> invoker) throws XAException
						{
							lock.lock();
							
							try
							{
								return strategy.invoke(proxy, invoker);
							}
							catch (XAException e)
							{
								lock.unlock();

								throw e;
							}
						}
					};
				}
			}
			
			Durability.Phase phase = phaseMap.get(method);
			
			if (phase != null)
			{
				final InvocationStrategy<XADataSource, XADataSourceDatabase, XAResource, Object, XAException> durabilityStrategy = this.cluster.getDurability().getInvocationStrategy(strategy, phase, new XidTransactionIdentifier(xid), this.getExceptionFactory());
				
				if (commit || rollback || forget)
				{
					final Lock lock = lockMap.remove(xid);

					return new InvocationStrategy<XADataSource, XADataSourceDatabase, XAResource, Object, XAException>()
					{
						@Override
						public Object invoke(SQLProxy<XADataSource, XADataSourceDatabase, XAResource, XAException> proxy, Invoker<XADataSource, XADataSourceDatabase, XAResource, Object, XAException> invoker) throws XAException
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
	protected Invoker<XADataSource, XADataSourceDatabase, XAResource, ?, XAException> getInvoker(XAResource object, Method method, Object[] parameters) throws XAException
	{
		Invoker<XADataSource, XADataSourceDatabase, XAResource, ?, XAException> invoker = super.getInvoker(object, method, parameters);
		
		Durability.Phase phase = phaseMap.get(method);
		
		if (method.equals(prepareMethod) || method.equals(commitMethod) || method.equals(rollbackMethod) || method.equals(forgetMethod))
		{
			Xid xid = (Xid) parameters[0];
			
			return this.cluster.getDurability().getInvoker(invoker, phase, new XidTransactionIdentifier(xid), this.getExceptionFactory());
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

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.SQLProxy#getExceptionFactory()
	 */
	@Override
	public ExceptionFactory<XAException> getExceptionFactory()
	{
		return XAExceptionFactory.getInstance();
	}
	
	private static class XidTransactionIdentifier implements TransactionIdentifier
	{
		private final Xid xid;
		
		XidTransactionIdentifier(Xid xid)
		{
			this.xid = xid;
		}
		
		@Override
		public byte[] getBytes()
		{
			return this.xid.getGlobalTransactionId();
		}
	}
}
