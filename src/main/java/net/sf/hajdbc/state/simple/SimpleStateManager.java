package net.sf.hajdbc.state.simple;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import net.sf.hajdbc.durability.InvocationEvent;
import net.sf.hajdbc.durability.InvokerEvent;
import net.sf.hajdbc.state.DatabaseEvent;
import net.sf.hajdbc.state.StateManager;

public class SimpleStateManager implements StateManager
{
	private final Map<InvocationEvent, Map<String, InvokerEvent>> invocations = new ConcurrentHashMap<InvocationEvent, Map<String, InvokerEvent>>();
	
	@Override
	public Set<String> getActiveDatabases()
	{
		return Collections.emptySet();
	}

	@Override
	public Map<InvocationEvent, Map<String, InvokerEvent>> recover()
	{
		return this.invocations;
	}

	@Override
	public void setActiveDatabases(Set<String> databases)
	{
	}

	@Override
	public void activated(DatabaseEvent event)
	{
	}

	@Override
	public void deactivated(DatabaseEvent event)
	{
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
		this.invocations.get(event).remove(event.getDatabaseId());
	}

	@Override
	public void beforeInvoker(InvokerEvent event)
	{
		this.invocations.get(event).put(event.getDatabaseId(), event);
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
