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
package net.sf.hajdbc.durability;

import java.util.Arrays;
import java.util.EventObject;


/**
 * @author paul
 *
 */
public class InvocationEvent extends EventObject
{
	private static final long serialVersionUID = -2703279764623112956L;
	
	private final Durability.Phase phase;
	
	public InvocationEvent(TransactionIdentifier transactionId, Durability.Phase phase)
	{
		super(transactionId.getBytes());
		
		this.phase = phase;
	}
	
	public Durability.Phase getPhase()
	{
		return this.phase;
	}
	
	public byte[] getTransactionId()
	{
		return (byte[]) this.source;
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object)
	{
		if ((object == null) || !(object instanceof EventObject)) return false;
		
		InvocationEvent event = (InvocationEvent) object;
		
		return (this.phase == event.phase) && Arrays.equals(this.getTransactionId(), event.getTransactionId());
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		return this.source.hashCode() + this.phase.ordinal();
	}
}
