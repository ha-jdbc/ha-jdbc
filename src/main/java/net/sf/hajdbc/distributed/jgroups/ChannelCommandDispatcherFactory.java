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
package net.sf.hajdbc.distributed.jgroups;

import net.sf.hajdbc.distributed.CommandDispatcher;
import net.sf.hajdbc.distributed.CommandDispatcherFactory;
import net.sf.hajdbc.distributed.MembershipListener;
import net.sf.hajdbc.distributed.Stateful;

/**
 * Factory for creating a JGroups instrumented command dispatcher.

 * @author Paul Ferraro
 */
public abstract class ChannelCommandDispatcherFactory implements ChannelProvider, CommandDispatcherFactory
{
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.distributed.CommandDispatcherFactory#createCommandDispatcher(java.lang.String, java.lang.Object, net.sf.hajdbc.distributed.Stateful, net.sf.hajdbc.distributed.MembershipListener)
	 */
	@Override
	public <C> CommandDispatcher<C> createCommandDispatcher(String id, C context, Stateful stateful, MembershipListener membershipListener) throws Exception
	{
		return new ChannelCommandDispatcher<C>(id, this, context, stateful, membershipListener);
	}
}
