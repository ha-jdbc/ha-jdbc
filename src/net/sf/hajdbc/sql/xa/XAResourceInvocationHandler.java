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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;

import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.LockManager;
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
public class XAResourceInvocationHandler extends AbstractChildInvocationHandler<XADataSource, XAConnection, XAResource>
{
	private static final Set<Method> driverReadMethodSet = Methods.findMethods(XAResource.class, "getTransactionTimeout", "isSameRM");
	private static final Set<Method> databaseWriteMethodSet = Methods.findMethods(XAResource.class, "setTransactionTimeout");
	private static final Set<Method> intraTransactionMethodSet = Methods.findMethods(XAResource.class, "end", "prepare", "recover");
	private static final Method startMethod = Methods.getMethod(XAResource.class, "start", Xid.class, Integer.TYPE);
	private static final Set<Method> endTransactionMethodSet = Methods.findMethods(XAResource.class, "commit", "rollback", "forget");
	
	// Xids are global - so store in static variable
	private static ConcurrentMap<Xid, Lock> lockMap = new ConcurrentHashMap<Xid, Lock>();
	
	/**
	 * @param connection
	 * @param proxy
	 * @param invoker
	 * @param objectMap
	 * @throws Exception
	 */
	protected XAResourceInvocationHandler(XAConnection connection, SQLProxy<XADataSource, XAConnection> proxy, Invoker<XADataSource, XAConnection, XAResource> invoker, Map<Database<XADataSource>, XAResource> objectMap) throws Exception
	{
		super(connection, proxy, invoker, XAResource.class, objectMap);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#getInvocationStrategy(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationStrategy<XADataSource, XAResource, ?> getInvocationStrategy(XAResource resource, Method method, Object[] parameters) throws Exception
	{
		if (driverReadMethodSet.contains(method))
		{
			return new DriverReadInvocationStrategy<XADataSource, XAResource, Object>();
		}
		
		if (databaseWriteMethodSet.contains(method))
		{
			return new DatabaseWriteInvocationStrategy<XADataSource, XAResource, Object>(this.cluster.getNonTransactionalExecutor());
		}
		
		if (method.equals(startMethod) || intraTransactionMethodSet.contains(method) || endTransactionMethodSet.contains(method))
		{
			final InvocationStrategy<XADataSource, XAResource, Object> strategy = new DatabaseWriteInvocationStrategy<XADataSource, XAResource, Object>(this.cluster.getTransactionalExecutor());
			
			if (method.equals(startMethod))
			{
				Xid xid = (Xid) parameters[0];
				
				final Lock lock = this.cluster.getLockManager().readLock(LockManager.GLOBAL);
				
				// Lock may already exist if we're resuming a suspended transaction
				Lock existingLock = lockMap.putIfAbsent(xid, lock);
				
				if (existingLock == null)
				{
					return new InvocationStrategy<XADataSource, XAResource, Object>()
					{
						@Override
						public Object invoke(SQLProxy<XADataSource, XAResource> proxy, Invoker<XADataSource, XAResource, Object> invoker) throws Exception
						{
							lock.lock();
							
							return strategy.invoke(proxy, invoker);
						}
					};
				}
			}
			
			if (endTransactionMethodSet.contains(method))
			{
				final Lock lock = lockMap.remove(parameters[0]);
				
				return new InvocationStrategy<XADataSource, XAResource, Object>()
				{
					@Override
					public Object invoke(SQLProxy<XADataSource, XAResource> proxy, Invoker<XADataSource, XAResource, Object> invoker) throws Exception
					{
						try
						{
							return strategy.invoke(proxy, invoker);
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
			
			return strategy;
		}
		
		return super.getInvocationStrategy(resource, method, parameters);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#close(java.lang.Object, java.lang.Object)
	 */
	@Override
	protected void close(XAConnection connection, XAResource resource)
	{
		// Do nothing
	}
}
