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
package net.sf.hajdbc.durability;

import net.sf.hajdbc.util.Event;

/**
 * @author Paul Ferraro
 */
public class DurabilityEventImpl extends Event<Object> implements DurabilityEvent
{
	private static final long serialVersionUID = -8747536263068408813L;
	
	private final Durability.Phase phase;
	
	public DurabilityEventImpl(Object transactionId, Durability.Phase phase)
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

	@Override
	public String toString()
	{
		return String.format("%s(%s)", this.getPhase(), this.source);
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object)
	{
		if ((object == null) || !(object instanceof DurabilityEvent)) return false;
		
		DurabilityEvent event = (DurabilityEvent) object;
		
		return (this.phase == event.getPhase()) && this.getTransactionId().equals(event.getTransactionId());
	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + (this.phase != null ? this.phase.hashCode() : 0);
		return result;
	}
}
