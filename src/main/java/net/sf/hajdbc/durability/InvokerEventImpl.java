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

/**
 * @author Paul Ferraro
 */
public class InvokerEventImpl extends DurabilityEventImpl implements InvokerEvent
{
	private static final long serialVersionUID = -1477607646901022715L;
	
	private final String databaseId;
	private InvokerResult result;
	
	/**
	 * Constructs a new InvokerEventImpl
	 * @param transactionId a transaction identifier
	 * @param phase the durability phase
	 * @param databaseId a database identifier
	 */
	public InvokerEventImpl(Object transactionId, Durability.Phase phase, String databaseId)
	{
		super(transactionId, phase);
		
		this.databaseId = databaseId;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.durability.InvokerEvent#getDatabaseId()
	 */
	@Override
	public String getDatabaseId()
	{
		return this.databaseId;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.durability.InvokerEvent#setResult(net.sf.hajdbc.durability.InvokerResult)
	 */
	@Override
	public void setResult(InvokerResult result)
	{
		this.result = result;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.durability.InvokerEvent#getResult()
	 */
	@Override
	public InvokerResult getResult()
	{
		return this.result;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.durability.DurabilityEventImpl#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object)
	{
		if ((object == null) || !(object instanceof InvokerEvent)) return false;
		
		InvokerEvent event = (InvokerEvent) object;
		
		return super.equals(object) && this.databaseId.equals(event.getDatabaseId());
	}
}
