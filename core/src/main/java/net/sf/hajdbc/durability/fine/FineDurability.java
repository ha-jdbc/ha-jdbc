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
package net.sf.hajdbc.durability.fine;

import java.util.Map;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.ExceptionFactory;
import net.sf.hajdbc.balancer.Balancer;
import net.sf.hajdbc.durability.DurabilityListener;
import net.sf.hajdbc.durability.InvocationEvent;
import net.sf.hajdbc.durability.InvokerEvent;
import net.sf.hajdbc.durability.InvokerEventImpl;
import net.sf.hajdbc.durability.InvokerResult;
import net.sf.hajdbc.durability.InvokerResultImpl;
import net.sf.hajdbc.durability.coarse.CoarseDurability;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.state.StateManager;
import net.sf.hajdbc.util.Objects;

/**
 * {@link net.sf.hajdbc.durability.Durability} implementation that tracks invocations as well as per-database invokers.
 * This durability level can both detect and recover from mid-commit crashes.
 * @author Paul Ferraro
 */
public class FineDurability<Z, D extends Database<Z>> extends CoarseDurability<Z, D>
{
	public FineDurability(DatabaseCluster<Z, D> cluster)
	{
		super(cluster);
	}

	@Override
	public InvokerEvent createInvokerEvent(Object transactionId, Phase phase, String databaseId)
	{
		return new InvokerEventImpl(transactionId, phase, databaseId);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.durability.none.NoDurability#getInvoker(net.sf.hajdbc.invocation.Invoker, net.sf.hajdbc.durability.Durability.Phase, java.lang.Object, net.sf.hajdbc.ExceptionFactory)
	 */
	@Override
	public <T, R, E extends Exception> Invoker<Z, D, T, R, E> getInvoker(final Invoker<Z, D, T, R, E> invoker, final Phase phase, final Object transactionId, final ExceptionFactory<E> exceptionFactory)
	{
		final DurabilityListener listener = this.cluster.getStateManager();
		
		return new Invoker<Z, D, T, R, E>()
		{
			@Override
			public R invoke(D database, T object) throws E
			{
				InvokerEvent event = new InvokerEventImpl(transactionId, phase, database.getId());
				
				listener.beforeInvoker(event);
				
				try
				{
					R result = invoker.invoke(database, object);
					
					event.setResult(new InvokerResultImpl(result));
					
					return result;
				}
				catch (Exception e)
				{
					event.setResult(new InvokerResultImpl(e));
					
					throw exceptionFactory.createException(e);
				}
				finally
				{
					listener.afterInvoker(event);
				}
			}
		};
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.durability.coarse.CoarseDurability#recover(java.util.Map)
	 */
	@Override
	public void recover(Map<InvocationEvent, Map<String, InvokerEvent>> map)
	{
		StateManager stateManager = this.cluster.getStateManager();
		Balancer<Z, D> balancer = this.cluster.getBalancer();
		D primary = balancer.primary();

		for (Map.Entry<InvocationEvent, Map<String, InvokerEvent>> entry: map.entrySet())
		{
			InvocationEvent invocation = entry.getKey();
			Map<String, InvokerEvent> invokers = entry.getValue();

			if (!invokers.isEmpty())
			{
				for (D backup: balancer.backups())
				{
					if (this.deactivateSlave(primary, backup, invocation, invokers))
					{
						this.cluster.deactivate(backup, stateManager);
					}
				}
			}
			
			stateManager.afterInvocation(invocation);
		}
	}
	
	private boolean deactivateSlave(D primary, D backup, InvocationEvent invocation, Map<String, InvokerEvent> invokers)
	{
		InvokerEvent primaryEvent = invokers.get(primary.getId());
		
		if (primaryEvent != null)
		{
			InvokerResult result = primaryEvent.getResult();
			
			if (result != null)
			{
				Object primaryValue = result.getValue();
				Exception primaryException = result.getException();
				
				InvokerEvent backupEvent = invokers.get(backup.getId());
				
				if (backupEvent != null)
				{
					InvokerResult backupResult = backupEvent.getResult();
					
					if (backupResult != null)
					{
						Object backupValue = backupResult.getValue();
						Exception backupException = backupResult.getException();
						
						if (primaryException != null)
						{
							if ((backupException == null) || !invocation.getExceptionType().getExceptionFactory().equals(primaryException, backupException))
							{
								return true;
							}
						}
						else if ((backupException != null) || !Objects.equals(primaryValue, backupValue))
						{
							return true;
						}
					}
					else
					{
						return true;
					}
				}
				else
				{
					return true;
				}
			}
			else
			{
				return true;
			}
		}
		else
		{
			if (invokers.containsKey(backup.getId()))
			{
				return true;
			}
		}
		
		return false;
	}
}
