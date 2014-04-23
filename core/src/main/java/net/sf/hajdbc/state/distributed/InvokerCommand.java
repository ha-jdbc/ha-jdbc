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
package net.sf.hajdbc.state.distributed;

import java.util.Map;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.distributed.Command;
import net.sf.hajdbc.durability.InvocationEvent;
import net.sf.hajdbc.durability.InvokerEvent;

public class InvokerCommand<Z, D extends Database<Z>> implements Command<Void, StateCommandContext<Z, D>>
{
	private static final long serialVersionUID = 5093904550015002207L;
	
	private final RemoteInvokerDescriptor descriptor;
	
	protected InvokerCommand(RemoteInvokerDescriptor descriptor)
	{
		this.descriptor = descriptor;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.distributed.Command#execute(java.lang.Object)
	 */
	@Override
	public Void execute(StateCommandContext<Z, D> context)
	{
		Map<InvocationEvent, Map<String, InvokerEvent>> invokers = context.getRemoteInvokers(this.descriptor);

		InvokerEvent event = this.descriptor.getEvent();
		String databaseId = event.getDatabaseId();
		
		synchronized (invokers)
		{
			Map<String, InvokerEvent> map = invokers.get(event);
			
			if (map != null)
			{
				map.put(databaseId, event);
			}
		}
		
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return String.format("%s(%s)", this.getClass().getSimpleName(), this.descriptor);
	}
}
