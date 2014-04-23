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
package net.sf.hajdbc.state;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import net.sf.hajdbc.ExceptionType;
import net.sf.hajdbc.durability.Durability;
import net.sf.hajdbc.durability.DurabilityEvent;
import net.sf.hajdbc.durability.DurabilityEventFactory;
import net.sf.hajdbc.durability.DurabilityListener;
import net.sf.hajdbc.durability.InvocationEvent;
import net.sf.hajdbc.durability.InvokerEvent;
import net.sf.hajdbc.tx.TransactionIdentifierFactory;
import net.sf.hajdbc.util.Objects;

/**
 * @author Paul Ferraro
 */
public class DurabilityListenerAdapter implements DurabilityListener, SerializedDurabilityEventFactory
{
	// TODO prevent memory leak
	// Cache serialized transaction identifiers
	private final ConcurrentMap<Object, byte[]> transactionIdentifiers = new ConcurrentHashMap<>();
	private final SerializedDurabilityListener listener;
	private final TransactionIdentifierFactory<Object> txIdFactory;
	private final DurabilityEventFactory eventFactory;

	@SuppressWarnings("unchecked")
	public DurabilityListenerAdapter(SerializedDurabilityListener listener, TransactionIdentifierFactory<? extends Object> txIdFactory, DurabilityEventFactory eventFactory)
	{
		this.listener = listener;
		this.txIdFactory = (TransactionIdentifierFactory<Object>) txIdFactory;
		this.eventFactory = eventFactory;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.durability.DurabilityListener#beforeInvocation(net.sf.hajdbc.durability.InvocationEvent)
	 */
	@Override
	public void beforeInvocation(InvocationEvent event)
	{
		Object transactionId = event.getTransactionId();
		byte[] txId = this.txIdFactory.serialize(transactionId);
		
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

	@Override
	public InvocationEvent createInvocationEvent(byte[] transactionId, byte phase, byte exceptionType)
	{
		return this.eventFactory.createInvocationEvent(this.txIdFactory.deserialize(transactionId), Durability.Phase.values()[phase], ExceptionType.values()[exceptionType]);
	}

	@Override
	public DurabilityEvent createEvent(byte[] transactionId, byte phase)
	{
		return this.eventFactory.createEvent(this.txIdFactory.deserialize(transactionId), Durability.Phase.values()[phase]);
	}
}