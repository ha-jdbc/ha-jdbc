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
package net.sf.hajdbc.state;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import net.sf.hajdbc.durability.DurabilityListener;
import net.sf.hajdbc.durability.InvocationEvent;
import net.sf.hajdbc.durability.InvokerEvent;
import net.sf.hajdbc.util.Objects;

/**
 * @author Paul Ferraro
 */
public class DurabilityListenerAdapter implements DurabilityListener
{
	// TODO prevent memory leak
	// Cache serialized transaction identifiers
	private final ConcurrentMap<Object, byte[]> transactionIdentifiers = new ConcurrentHashMap<Object, byte[]>();
	private final SerializedDurabilityListener listener;
	
	public DurabilityListenerAdapter(SerializedDurabilityListener listener)
	{
		this.listener = listener;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.durability.DurabilityListener#beforeInvocation(net.sf.hajdbc.durability.InvocationEvent)
	 */
	@Override
	public void beforeInvocation(InvocationEvent event)
	{
		Object transactionId = event.getTransactionId();
		byte[] txId = Objects.serialize(transactionId);
		
		this.transactionIdentifiers.put(transactionId, txId);
		this.listener.beforeInvocation(txId, (byte) event.getPhase().ordinal(), (byte) event.getExceptionType().ordinal());
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.durability.DurabilityListener#afterInvocation(net.sf.hajdbc.durability.InvocationEvent)
	 */
	@Override
	public void afterInvocation(InvocationEvent event)
	{
		this.listener.afterInvocation(this.transactionIdentifiers.remove(event.getTransactionId()), (byte) event.getPhase().ordinal());
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.durability.DurabilityListener#beforeInvoker(net.sf.hajdbc.durability.InvokerEvent)
	 */
	@Override
	public void beforeInvoker(InvokerEvent event)
	{
		this.listener.beforeInvoker(this.transactionIdentifiers.get(event.getTransactionId()), (byte) event.getPhase().ordinal(), event.getDatabaseId());
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.durability.DurabilityListener#afterInvoker(net.sf.hajdbc.durability.InvokerEvent)
	 */
	@Override
	public void afterInvoker(InvokerEvent event)
	{
		this.listener.afterInvoker(this.transactionIdentifiers.get(event.getTransactionId()), (byte) event.getPhase().ordinal(), event.getDatabaseId(), Objects.serialize(event.getResult()));
	}
}