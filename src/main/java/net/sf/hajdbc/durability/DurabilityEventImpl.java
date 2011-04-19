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
package net.sf.hajdbc.durability;

import java.util.EventObject;

/**
 * @author Paul Ferraro
 */
public class DurabilityEventImpl extends EventObject implements DurabilityEvent
{
	private static final long serialVersionUID = -8747536263068408813L;
	
	private final Durability.Phase phase;
	
	protected DurabilityEventImpl(Object transactionId, Durability.Phase phase)
	{
		super(transactionId);
		
		this.phase = phase;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.durability.DurabilityEvent#getTransactionId()
	 */
	@Override
	public Object getTransactionId()
	{
		return this.source;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.durability.DurabilityEvent#getPhase()
	 */
	@Override
	public Durability.Phase getPhase()
	{
		return this.phase;
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object)
	{
		if ((object == null) || !(object instanceof InvocationEvent)) return false;
		
		InvocationEvent event = (InvocationEvent) object;
		
		return (this.phase == event.getPhase()) && this.source.equals(event.getTransactionId());
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		return this.source.hashCode();
	}
}
