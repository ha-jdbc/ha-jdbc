package net.sf.hajdbc.state.simple;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.durability.InvocationEvent;
import net.sf.hajdbc.durability.InvokerEvent;
import net.sf.hajdbc.state.DatabaseEvent;
import net.sf.hajdbc.state.StateManager;
import net.sf.hajdbc.state.StateManagerFactory;

public class SimpleStateManagerFactory implements StateManager, StateManagerFactory
{
	private final Map<InvocationEvent, Map<String, InvokerEvent>> invocations = new ConcurrentHashMap<InvocationEvent, Map<String, InvokerEvent>>();
	private final Set<String> activeDatabases = new CopyOnWriteArraySet<String>();
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.state.StateManagerFactory#createStateManager(net.sf.hajdbc.DatabaseCluster)
	 */
	@Override
	public <Z, D extends Database<Z>> StateManager createStateManager(DatabaseCluster<Z, D> cluster)
	{
		return this;
	}

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
