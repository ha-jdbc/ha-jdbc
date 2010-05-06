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

import java.util.Map;
import java.util.SortedMap;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.durability.DurabilityListener;
import net.sf.hajdbc.durability.InvocationEvent;
import net.sf.hajdbc.durability.InvokerEvent;
import net.sf.hajdbc.durability.TransactionIdentifier;
import net.sf.hajdbc.durability.none.NoDurability;
import net.sf.hajdbc.sql.InvocationStrategy;
import net.sf.hajdbc.sql.Invoker;
import net.sf.hajdbc.sql.SQLProxy;
import net.sf.hajdbc.state.StateManager;

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
	 * 
	 * @see net.sf.hajdbc.durability.Durability#getInvocationStrategy(net.sf.hajdbc.sql.InvocationStrategy)
	 */
	@Override
	public InvocationStrategy getInvocationStrategy(final InvocationStrategy strategy, final Phase phase, final TransactionIdentifier transactionId)
	{
		final DurabilityListener listener = this.cluster.getStateManager();

		return new InvocationStrategy()
		{
			@Override
			public <ZZ, DD extends Database<ZZ>, T, R, EE extends Exception> SortedMap<DD, R> invoke(SQLProxy<ZZ, DD, T, EE> proxy, Invoker<ZZ, DD, T, R, EE> invoker) throws EE
			{
				InvocationEvent event = new InvocationEvent(transactionId, phase);

				listener.beforeInvocation(event);

				try
				{
					return strategy.invoke(proxy, invoker);
				}
				catch (Exception e)
				{
					throw proxy.getExceptionFactory().createException(e);
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
	 * 
	 * @see net.sf.hajdbc.durability.Durability#recover(net.sf.hajdbc.balancer.Balancer,
	 *      java.util.Map)
	 */
	@Override
	public void recover(Map<InvocationEvent, Map<String, InvokerEvent>> invokers)
	{
		StateManager stateManager = this.cluster.getStateManager();

		for (D database: this.cluster.getBalancer().slaves())
		{
			this.cluster.deactivate(database, stateManager);
		}

		for (InvocationEvent event: invokers.keySet())
		{
			stateManager.afterInvocation(event);
		}
	}
}
