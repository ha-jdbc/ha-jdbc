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
package net.sf.hajdbc.durability.coarse;

import java.util.Iterator;
import java.util.Map;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.ExceptionFactory;
import net.sf.hajdbc.durability.DurabilityListener;
import net.sf.hajdbc.durability.InvocationEvent;
import net.sf.hajdbc.durability.InvokerEvent;
import net.sf.hajdbc.durability.TransactionIdentifier;
import net.sf.hajdbc.durability.none.NoDurability;
import net.sf.hajdbc.sql.InvocationStrategy;
import net.sf.hajdbc.sql.Invoker;
import net.sf.hajdbc.sql.SQLProxy;

/**
 * @author paul
 *
 */
public class CoarseDurability<Z, D extends Database<Z>> extends NoDurability<Z, D>
{
	protected final DatabaseCluster<Z, D> cluster;
	
	public CoarseDurability(DatabaseCluster<Z, D> cluster)
	{
		this.cluster = cluster;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.durability.Durability#getInvocationStrategy(net.sf.hajdbc.sql.InvocationStrategy)
	 */
	@Override
	public <T, R, E extends Exception> InvocationStrategy<Z, D, T, R, E> getInvocationStrategy(final InvocationStrategy<Z, D, T, R, E> strategy, final Phase phase, final TransactionIdentifier transactionId, final ExceptionFactory<E> exceptionFactory)
	{
		final DurabilityListener listener = this.cluster.getStateManager();
		
		return new InvocationStrategy<Z, D, T, R, E>()
		{
			@Override
			public R invoke(SQLProxy<Z, D, T, E> proxy, Invoker<Z, D, T, R, E> invoker) throws E
			{
				InvocationEvent event = new InvocationEvent(transactionId, phase);
				
				listener.beforeInvocation(event);
				
				try
				{
					R result = strategy.invoke(proxy, invoker);
					
					return result;
				}
				catch (Exception e)
				{
					E exception = exceptionFactory.createException(e);
					
					throw exception;
				}
				finally
				{
					listener.afterInvocation(event);
				}
			}
		};
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.durability.Durability#recover(net.sf.hajdbc.balancer.Balancer, java.util.Map)
	 */
	@Override
	public void recover(Map<InvocationEvent, Map<String, InvokerEvent>> invokers)
	{
		Iterator<D> databases = this.cluster.getBalancer().iterator();
		
		if (databases.hasNext())
		{
			// Keep master active
			databases.next();
			
			while (databases.hasNext())
			{
				this.cluster.deactivate(databases.next(), this.cluster.getStateManager());
			}
		}
		
		DurabilityListener listener = this.cluster.getStateManager();
		
		for (InvocationEvent event: invokers.keySet())
		{
			listener.afterInvocation(event);
		}
	}
}
