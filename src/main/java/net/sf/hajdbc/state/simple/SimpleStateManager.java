package net.sf.hajdbc.state.simple;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import net.sf.hajdbc.durability.InvocationEvent;
import net.sf.hajdbc.durability.InvokerEvent;
import net.sf.hajdbc.state.DatabaseEvent;
import net.sf.hajdbc.state.StateManager;

public class SimpleStateManager implements StateManager
{
	private final Set<String> activeDatabases = new HashSet<String>();
	private final Map<InvocationEvent, Map<String, InvokerEvent>> invocations = new ConcurrentHashMap<InvocationEvent, Map<String, InvokerEvent>>();
	
	@Override
	public Set<String> getActiveDatabases()
	{
		synchronized (this.activeDatabases)
		{
			return new TreeSet<String>(this.activeDatabases);
		}
	}

	@Override
	public Map<InvocationEvent, Map<String, InvokerEvent>> recover()
	{
		return this.invocations;
	}

	@Override
	public void setActiveDatabases(Set<String> databases)
	{
		synchronized (this.activeDatabases)
		{
			this.activeDatabases.clear();
			this.activeDatabases.addAll(databases);
		}
	}

	@Override
	public void activated(DatabaseEvent event)
	{
		synchronized (this.activeDatabases)
		{
			this.activeDatabases.add(event.getDatabaseId());
		}
	}

	@Override
	public void deactivated(DatabaseEvent event)
	{
		synchronized (this.activeDatabases)
		{
			this.activeDatabases.remove(event.getDatabaseId());
		}
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
