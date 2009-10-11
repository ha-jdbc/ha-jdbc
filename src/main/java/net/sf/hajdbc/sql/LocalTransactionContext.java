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

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.durability.Durability;
import net.sf.hajdbc.durability.TransactionIdentifier;
import net.sf.hajdbc.lock.LockManager;

/**
 * @author Paul Ferraro
 * @param <D> 
 */
public class LocalTransactionContext<Z, D extends Database<Z>> implements TransactionContext<Z, D>
{
	private static final AtomicLong transactionCounter = new AtomicLong();
	
	final Durability<Z, D> durability;
	private final Lock lock;
	volatile TransactionIdentifier transactionId;
	
	/**
	 * @param cluster
	 */
	public LocalTransactionContext(DatabaseCluster<Z, D> cluster)
	{
		this.lock = cluster.getLockManager().readLock(LockManager.GLOBAL);
		this.durability = cluster.getDurability();
	}
	
	/**
	 * @see net.sf.hajdbc.sql.TransactionContext#start(net.sf.hajdbc.sql.InvocationStrategy, java.sql.Connection)
	 */
	public <T, R> InvocationStrategy<Z, D, T, R, SQLException> start(final InvocationStrategy<Z, D, T, R, SQLException> strategy, Connection connection) throws SQLException
	{
		if (this.transactionId != null) return strategy;

		if (connection.getAutoCommit())
		{
			return new InvocationStrategy<Z, D, T, R, SQLException>()
			{
				@Override
				public R invoke(SQLProxy<Z, D, T, SQLException> proxy, Invoker<Z, D, T, R, SQLException> invoker) throws SQLException
				{
					LocalTransactionContext.this.lock();
					
					try
					{
						return LocalTransactionContext.this.durability.getInvocationStrategy(strategy, Durability.Phase.COMMIT, LocalTransactionContext.this.transactionId, SQLExceptionFactory.getInstance()).invoke(proxy, invoker);
					}
					finally
					{
						LocalTransactionContext.this.unlock();
					}
				}
			};
		}
		
		return new InvocationStrategy<Z, D, T, R, SQLException>()
		{
			@Override
			public R invoke(SQLProxy<Z, D, T, SQLException> proxy, Invoker<Z, D, T, R, SQLException> invoker) throws SQLException
			{
				LocalTransactionContext.this.lock();
				
				try
				{
					return strategy.invoke(proxy, invoker);
				}
				catch (Throwable e)
				{
					LocalTransactionContext.this.unlock();
					
					throw proxy.getExceptionFactory().createException(e);
				}
			}
		};
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.TransactionContext#start(net.sf.hajdbc.sql.Invoker, java.sql.Connection)
	 */
	@Override
	public <T, R> Invoker<Z, D, T, R, SQLException> start(final Invoker<Z, D, T, R, SQLException> invoker, Connection connection) throws SQLException
	{
		if ((this.transactionId == null) || !connection.getAutoCommit()) return invoker;

		return new Invoker<Z, D, T, R, SQLException>()
		{
			@Override
			public R invoke(D database, T object) throws SQLException
			{
				return LocalTransactionContext.this.durability.getInvoker(invoker, Durability.Phase.COMMIT, LocalTransactionContext.this.transactionId, SQLExceptionFactory.getInstance()).invoke(database, object);
			}
		};
	}

	/**
	 * @see net.sf.hajdbc.sql.TransactionContext#end(net.sf.hajdbc.sql.InvocationStrategy)
	 */
	public <T, R> InvocationStrategy<Z, D, T, R, SQLException> end(InvocationStrategy<Z, D, T, R, SQLException> strategy, Durability.Phase phase)
	{
		if (this.transactionId == null) return strategy;

		final InvocationStrategy<Z, D, T, R, SQLException> durabilityStrategy = this.durability.getInvocationStrategy(strategy, phase, this.transactionId, SQLExceptionFactory.getInstance());
		
		return new InvocationStrategy<Z, D, T, R, SQLException>()
		{
			@Override
			public R invoke(SQLProxy<Z, D, T, SQLException> proxy, Invoker<Z, D, T, R, SQLException> invoker) throws SQLException
			{
				try
				{
					return durabilityStrategy.invoke(proxy, invoker);
				}
				finally
				{
					LocalTransactionContext.this.unlock();
				}
			}
		};
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.TransactionContext#end(net.sf.hajdbc.sql.Invoker)
	 */
	@Override
	public <T, R> Invoker<Z, D, T, R, SQLException> end(final Invoker<Z, D, T, R, SQLException> invoker, Durability.Phase phase) throws SQLException
	{
		if (this.transactionId == null) return invoker;

		return this.durability.getInvoker(invoker, phase, this.transactionId, SQLExceptionFactory.getInstance());
	}

	/**
	 * @see net.sf.hajdbc.sql.TransactionContext#close()
	 */
	@Override
	public void close()
	{
		// Tsk, tsk... User neglected to commit/rollback transaction
		if (this.transactionId != null)
		{
			this.unlock();
		}
	}

	void lock()
	{
		this.lock.lock();
		this.transactionId = new LocalTransactionIdentifier(transactionCounter.incrementAndGet());
	}
	
	void unlock()
	{
		this.lock.unlock();
		this.transactionId = null;
	}
	
	private static class LocalTransactionIdentifier implements TransactionIdentifier
	{
		private static final int LONG_BYTES = Long.SIZE / Byte.SIZE;
		
		private final long id;
		
		LocalTransactionIdentifier(long id)
		{
			this.id = id;
		}
		
		/**
		 * {@inheritDoc}
		 * @see net.sf.hajdbc.TransactionIdentifierSerializer#getBytes(java.lang.Object)
		 */
		@Override
		public byte[] getBytes()
		{
			ByteBuffer buffer = ByteBuffer.allocate(LONG_BYTES);
			buffer.putLong(this.id);
			return buffer.array();
		}
	}
}