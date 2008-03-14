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
package net.sf.hajdbc.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.LockManager;

/**
 * @author Paul Ferraro
 * @param <D> 
 */
public class LocalTransactionContext<D> implements TransactionContext<D>
{
	private Lock lock;
	private boolean locked = false;
	
	/**
	 * @param cluster
	 */
	public LocalTransactionContext(DatabaseCluster<D> cluster)
	{
		this.lock = cluster.getLockManager().readLock(LockManager.GLOBAL);
	}
	
	/**
	 * @see net.sf.hajdbc.sql.TransactionContext#start(net.sf.hajdbc.sql.InvocationStrategy, java.sql.Connection)
	 */
	public <T, R> InvocationStrategy<D, T, R> start(final InvocationStrategy<D, T, R> strategy, Connection connection) throws SQLException
	{
		if (this.locked) return strategy;

		if (connection.getAutoCommit())
		{
			return new LockingInvocationStrategy<D, T, R>(strategy, Collections.singletonList(this.lock));
		}
		
		return new InvocationStrategy<D, T, R>()
		{
			@Override
			public R invoke(SQLProxy<D, T> proxy, Invoker<D, T, R> invoker) throws Exception
			{
				LocalTransactionContext.this.lock();
				
				try
				{
					return strategy.invoke(proxy, invoker);
				}
				catch (Exception e)
				{
					LocalTransactionContext.this.unlock();
					
					throw e;
				}
			}
		};
	}
	
	/**
	 * @see net.sf.hajdbc.sql.TransactionContext#end(net.sf.hajdbc.sql.InvocationStrategy)
	 */
	public <T, R> InvocationStrategy<D, T, R> end(final InvocationStrategy<D, T, R> strategy)
	{
		if (!this.locked) return strategy;
		
		return new InvocationStrategy<D, T, R>()
		{
			@Override
			public R invoke(SQLProxy<D, T> proxy, Invoker<D, T, R> invoker) throws Exception
			{
				try
				{
					return strategy.invoke(proxy, invoker);
				}
				finally
				{
					LocalTransactionContext.this.unlock();
				}
			}
		};
	}

	/**
	 * @see net.sf.hajdbc.sql.TransactionContext#close()
	 */
	@Override
	public void close()
	{
		// Tsk, tsk... User neglected to commit/rollback transaction
		if (this.locked)
		{
			this.unlock();
		}
	}
	
	void lock()
	{
		this.lock.lock();
		this.locked = true;
	}
	
	void unlock()
	{
		this.lock.unlock();
		this.locked = false;
	}
}