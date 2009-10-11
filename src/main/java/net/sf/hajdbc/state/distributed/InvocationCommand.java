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
package net.sf.hajdbc.state.distributed;

import java.util.Map;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.distributed.Command;
import net.sf.hajdbc.durability.InvocationEvent;
import net.sf.hajdbc.durability.InvokerEvent;

/**
 * @author paul
 *
 */
public abstract class InvocationCommand<Z, D extends Database<Z>> implements Command<Void, StateCommandContext<Z, D>>
{
	private static final long serialVersionUID = -1876128495499915710L;
	
	private final RemoteInvocationDescriptor descriptor;
	
	protected InvocationCommand(RemoteInvocationDescriptor descriptor)
	{
		this.descriptor = descriptor;
	}
	
	@Override
	public Void execute(StateCommandContext<Z, D> context)
	{
		Map<InvocationEvent, Map<D, InvokerEvent>> invokers = context.getRemoteInvokers(this.descriptor);

		synchronized (invokers)
		{
			this.execute(invokers, this.descriptor.getEvent());
		}
		
		return null;
	}

	protected abstract void execute(Map<InvocationEvent, Map<D, InvokerEvent>> invokers, InvocationEvent event);
	
	@Override
	public Object marshalResult(Void result)
	{
		return null;
	}

	@Override
	public Void unmarshalResult(Object object)
	{
		return null;
	}
}
