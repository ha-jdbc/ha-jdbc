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
package net.sf.hajdbc.state.simple;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import net.sf.hajdbc.durability.InvocationEvent;
import net.sf.hajdbc.durability.InvocationEventAdapter;
import net.sf.hajdbc.durability.InvokerEvent;
import net.sf.hajdbc.state.DatabaseEvent;
import net.sf.hajdbc.state.StateManager;

public class SimpleStateManager implements StateManager
{
	private final Map<InvocationEvent, Map<String, InvokerEvent>> invocations = new ConcurrentHashMap<InvocationEvent, Map<String, InvokerEvent>>();
	private final Set<String> activeDatabases = new CopyOnWriteArraySet<String>();

	@Override
	public Set<String> getActiveDatabases()
	{
		return this.activeDatabases;
	}

	@Override
	public Map<InvocationEvent, Map<String, InvokerEvent>> recover()
	{
		return this.invocations;
	}

	@Override
	public void setActiveDatabases(Set<String> databases)
	{
		this.activeDatabases.retainAll(databases);
		this.activeDatabases.addAll(databases);
	}

	@Override
	public void activated(DatabaseEvent event)
	{
		this.activeDatabases.add(event.getSource());
	}

	@Override
	public void deactivated(DatabaseEvent event)
	{
		this.activeDatabases.remove(event.getSource());
	}

	@Override
	public void afterInvocation(InvocationEvent event)
	{
		this.invocations.remove(event);
	}

	@Override
	public void beforeInvocation(InvocationEvent event)
	{
		this.invocations.put(event, new HashMap<String, InvokerEvent>());
	}

	@Override
	public void afterInvoker(InvokerEvent event)
	{
		this.invocations.get(new InvocationEventAdapter(event)).remove(event.getDatabaseId());
	}

	@Override
	public void beforeInvoker(InvokerEvent event)
	{
		this.invocations.get(new InvocationEventAdapter(event)).put(event.getDatabaseId(), event);
	}

	@Override
	public boolean isEnabled()
	{
		return true;
	}

	@Override
	public void start()
	{
	}

	@Override
	public void stop()
	{
	}
}
