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
package net.sf.hajdbc.durability.fine;

import java.util.Iterator;
import java.util.Map;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.ExceptionFactory;
import net.sf.hajdbc.balancer.Balancer;
import net.sf.hajdbc.durability.DurabilityListener;
import net.sf.hajdbc.durability.InvocationEvent;
import net.sf.hajdbc.durability.InvokerEvent;
import net.sf.hajdbc.durability.TransactionIdentifier;
import net.sf.hajdbc.durability.coarse.CoarseDurability;
import net.sf.hajdbc.sql.Invoker;
import net.sf.hajdbc.state.StateManager;

/**
 * @author paul
 *
 */
public class FineDurability<Z, D extends Database<Z>> extends CoarseDurability<Z, D>
{
	public FineDurability(DatabaseCluster<Z, D> cluster)
	{
		super(cluster);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.durability.Durability#getInvoker(net.sf.hajdbc.sql.Invoker)
	 */
	@Override
	public <T, R, E extends Exception> Invoker<Z, D, T, R, E> getInvoker(final Invoker<Z, D, T, R, E> invoker, final Phase phase, final TransactionIdentifier transactionId, final ExceptionFactory<E> exceptionFactory)
	{
		final DurabilityListener listener = this.cluster.getStateManager();
		
		return new Invoker<Z, D, T, R, E>()
		{
			@Override
			public R invoke(D database, T object) throws E
			{
				InvokerEvent event = new InvokerEvent(transactionId, phase, database);
				
				listener.beforeInvoker(event);
				
				try
				{
					R result = invoker.invoke(database, object);
					
					event.setResult(result);
					
					return result;
				}
				catch (Exception e)
				{
					E exception = exceptionFactory.createException(e);

					event.setResult(exception);
					
					throw exception;
				}
				finally
				{
					listener.afterInvoker(event);
				}
			}
		};
	}

	@Override
	public void recover(Map<InvocationEvent, Map<String, InvokerEvent>> map)
	{
		StateManager stateManager = this.cluster.getStateManager();
		Balancer<Z, D> balancer = this.cluster.getBalancer();
		D master = balancer.master();

		for (Map.Entry<InvocationEvent, Map<String, InvokerEvent>> entry: map.entrySet())
		{
			InvocationEvent invocation = entry.getKey();
			Map<String, InvokerEvent> invokers = entry.getValue();

			if (!invokers.isEmpty())
			{
				for (D slave: balancer.slaves())
				{
					if (this.deactivateSlave(master, slave, invokers))
					{
						this.cluster.deactivate(slave, stateManager);
					}
				}
			}
			
			stateManager.afterInvocation(invocation);
		}
		
		for (Map<String, InvokerEvent> invokers: map.values())
		{
			if (!invokers.isEmpty())
			{
				Iterator<D> databases = balancer.iterator();
				
				if (databases.hasNext())
				{
					databases.next();
					
					while (databases.hasNext())
					{
						
					}
				}
				
				for (Map.Entry<String, InvokerEvent> invokerEntry: invokers.entrySet())
				{
					String databaseId = invokerEntry.getKey();
					InvokerEvent invoker = invokerEntry.getValue();
					
					switch (invoker.getPhase())
					{
						case COMMIT:
						{
							break;
						}
						case ROLLBACK:
						{
							break;
						}
						case PREPARE:
						{
							// Need to check for heuristic decisions
							break;
						}
						case FORGET:
						{
							break;
						}
					}
				}
			}
		}
	}
	
	private boolean deactivateSlave(D master, D slave, Map<String, InvokerEvent> invokers)
	{
		InvokerEvent masterEvent = invokers.get(master.getId());
		
		if (masterEvent != null)
		{
			if (masterEvent.isCompleted())
			{
				byte[] masterResult = masterEvent.getResult();
				byte[] masterException = masterEvent.getException();
				
				InvokerEvent slaveEvent = invokers.get(slave.getId());
				
				if (slaveEvent != null)
				{
					if (slaveEvent.isCompleted())
					{
						byte[] slaveResult = slaveEvent.getResult();
						byte[] slaveException = slaveEvent.getException();
						
						if ((masterResult != slaveResult) && ((masterResult == null) || (slaveResult == null) || masterResult.equals(slaveResult)))
						{
							return true;
						}
						else if ((masterException != slaveException) && ((masterException == null) || (slaveException == null)))
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
			if (invokers.containsKey(slave.getId()))
			{
				return true;
			}
		}
		
		return false;
	}
}
