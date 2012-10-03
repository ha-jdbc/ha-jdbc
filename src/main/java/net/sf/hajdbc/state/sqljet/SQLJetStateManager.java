/*
 * HA-JDBC: High-Availablity JDBC
 * Copyright 2010 Paul Ferraro
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
package net.sf.hajdbc.state.sqljet;

import java.util.Map;
import java.util.Set;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.durability.DurabilityListener;
import net.sf.hajdbc.durability.InvocationEvent;
import net.sf.hajdbc.durability.InvokerEvent;
import net.sf.hajdbc.state.DatabaseEvent;
import net.sf.hajdbc.state.DurabilityListenerAdapter;
import net.sf.hajdbc.state.SerializedDurabilityListener;
import net.sf.hajdbc.state.StateManager;

/**
 * @author Paul Ferraro
 */
public class SQLJetStateManager<Z, D extends Database<Z>> implements StateManager, SerializedDurabilityListener
{
	private final DurabilityListener listener;

	public SQLJetStateManager(DatabaseCluster<Z, D> cluster)
	{
		this.listener = new DurabilityListenerAdapter(this, cluster.getTransactionIdentifierFactory());
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseClusterListener#activated(net.sf.hajdbc.state.DatabaseEvent)
	 */
	@Override
	public void activated(DatabaseEvent event)
	{
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseClusterListener#deactivated(net.sf.hajdbc.state.DatabaseEvent)
	 */
	@Override
	public void deactivated(DatabaseEvent event)
	{
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.durability.DurabilityListener#beforeInvocation(net.sf.hajdbc.durability.InvocationEvent)
	 */
	@Override
	public void beforeInvocation(InvocationEvent event)
	{
		this.listener.beforeInvocation(event);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.durability.DurabilityListener#afterInvocation(net.sf.hajdbc.durability.InvocationEvent)
	 */
	@Override
	public void afterInvocation(InvocationEvent event)
	{
		this.listener.afterInvocation(event);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.durability.DurabilityListener#beforeInvoker(net.sf.hajdbc.durability.InvokerEvent)
	 */
	@Override
	public void beforeInvoker(InvokerEvent event)
	{
		this.listener.beforeInvoker(event);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.durability.DurabilityListener#afterInvoker(net.sf.hajdbc.durability.InvokerEvent)
	 */
	@Override
	public void afterInvoker(InvokerEvent event)
	{
		this.listener.afterInvoker(event);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.Lifecycle#start()
	 */
	@Override
	public void start() throws Exception
	{
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.Lifecycle#stop()
	 */
	@Override
	public void stop()
	{
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.state.StateManager#getActiveDatabases()
	 */
	@Override
	public Set<String> getActiveDatabases()
	{
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.state.StateManager#setActiveDatabases(java.util.Set)
	 */
	@Override
	public void setActiveDatabases(Set<String> databases)
	{
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.state.StateManager#recover()
	 */
	@Override
	public Map<InvocationEvent, Map<String, InvokerEvent>> recover()
	{
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.state.SerializedDurabilityListener#beforeInvocation(byte[], byte, byte)
	 */
	@Override
	public void beforeInvocation(byte[] transactionId, byte phase, byte exceptionType)
	{
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.state.SerializedDurabilityListener#afterInvocation(byte[], byte)
	 */
	@Override
	public void afterInvocation(byte[] transactionId, byte phase)
	{
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.state.SerializedDurabilityListener#beforeInvoker(byte[], byte, java.lang.String)
	 */
	@Override
	public void beforeInvoker(byte[] transactionId, byte phase, String databaseId)
	{
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.state.SerializedDurabilityListener#afterInvoker(byte[], byte, java.lang.String, byte[])
	 */
	@Override
	public void afterInvoker(byte[] transactionId, byte phase, String databaseId, byte[] result)
	{
	}
}
