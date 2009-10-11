/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2009 Paul Ferraro
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
package net.sf.hajdbc.durability.coarse;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.ExceptionFactory;
import net.sf.hajdbc.durability.DurabilityListener;
import net.sf.hajdbc.durability.InvocationEvent;
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
	final DurabilityListener listener;
	
	public CoarseDurability(DurabilityListener listener)
	{
		this.listener = listener;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.durability.Durability#getInvocationStrategy(net.sf.hajdbc.sql.InvocationStrategy)
	 */
	@Override
	public <T, R, E extends Exception> InvocationStrategy<Z, D, T, R, E> getInvocationStrategy(final InvocationStrategy<Z, D, T, R, E> strategy, final Phase phase, final TransactionIdentifier transactionId, final ExceptionFactory<E> exceptionFactory)
	{
		return new InvocationStrategy<Z, D, T, R, E>()
		{
			@Override
			public R invoke(SQLProxy<Z, D, T, E> proxy, Invoker<Z, D, T, R, E> invoker) throws E
			{
				InvocationEvent event = new InvocationEvent(transactionId, phase);
				
				CoarseDurability.this.listener.beforeInvocation(event);
				
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
					CoarseDurability.this.listener.afterInvocation(event);
				}
			}
		};
	}
}
