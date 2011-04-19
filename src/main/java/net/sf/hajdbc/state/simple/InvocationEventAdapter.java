/*
 * HA-JDBC: High-Availablity JDBC
 * Copyright 2011 Paul Ferraro
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
package net.sf.hajdbc.state.simple;

import net.sf.hajdbc.ExceptionType;
import net.sf.hajdbc.durability.DurabilityEvent;
import net.sf.hajdbc.durability.DurabilityEventImpl;
import net.sf.hajdbc.durability.InvocationEvent;

/**
 * @author Paul Ferraro
 */
public class InvocationEventAdapter extends DurabilityEventImpl implements InvocationEvent
{
	private static final long serialVersionUID = -2771937925436783287L;

	public InvocationEventAdapter(DurabilityEvent event)
	{
		super(event.getTransactionId(), event.getPhase());
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.durability.InvocationEvent#getExceptionType()
	 */
	@Override
	public ExceptionType getExceptionType()
	{
		return null;
	}
}
