package net.sf.hajdbc.state.distributed;

import java.util.Map;
import java.util.TreeMap;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.durability.InvocationEvent;
import net.sf.hajdbc.durability.InvokerEvent;

public class PreInvocationCommand<Z, D extends Database<Z>> extends InvocationCommand<Z, D>
{
	private static final long serialVersionUID = -1876128495499915710L;
	
	public PreInvocationCommand(RemoteInvocationDescriptor descriptor)
	{
		super(descriptor);
	}

	@Override
	protected void execute(Map<InvocationEvent, Map<D, InvokerEvent>> invokers, InvocationEvent event)
	{
		invokers.put(event, new TreeMap<D, InvokerEvent>());
	}
}
