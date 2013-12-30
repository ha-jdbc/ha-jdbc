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
package net.sf.hajdbc.distributed.jgroups;

import net.sf.hajdbc.distributed.CommandDispatcher;
import net.sf.hajdbc.distributed.CommandDispatcherFactory;
import net.sf.hajdbc.distributed.MembershipListener;
import net.sf.hajdbc.distributed.Stateful;

import org.jgroups.JChannel;

/**
 * Factory for creating a JGroups instrumented command dispatcher.

 * @author Paul Ferraro
 */
public class JGroupsCommandDispatcherFactory implements CommandDispatcherFactory
{
	private static final long serialVersionUID = 5135621114239237376L;
	
	public static final long DEFAULT_TIMEOUT = 60000;
	public static final String DEFAULT_STACK = "udp.xml";
	
	private String stack = DEFAULT_STACK;
	private long timeout = DEFAULT_TIMEOUT;

	@Override
	public String getId()
	{
		return "jgroups";
	}

	@Override
	public <C> CommandDispatcher<C> createCommandDispatcher(String id, C context, Stateful stateful, MembershipListener membershipListener) throws Exception
	{
		return new JGroupsCommandDispatcher<C>(id, new JChannel(this.stack), this.timeout, context, stateful, membershipListener);
	}
	
	public void setStack(String stack)
	{
		this.stack = stack;
	}
	
	public void setTimeout(long timeout)
	{
		this.timeout = timeout;
	}
}
