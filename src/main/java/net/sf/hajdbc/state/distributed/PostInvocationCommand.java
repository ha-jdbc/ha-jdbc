package net.sf.hajdbc.state.distributed;

import java.util.Map;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.durability.InvocationEvent;
import net.sf.hajdbc.durability.InvokerEvent;

public class PostInvocationCommand<Z, D extends Database<Z>> extends InvocationCommand<Z, D>
{
	private static final long serialVersionUID = 6851682187122656940L;

	public PostInvocationCommand(RemoteInvocationDescriptor descriptor)
	{
		super(descriptor);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.state.distributed.InvocationCommand#execute(java.util.Map, net.sf.hajdbc.durability.InvocationEvent)
	 */
	@Override
	protected void execute(Map<InvocationEvent, Map<D, InvokerEvent>> invokers, InvocationEvent event)
	{
		invokers.remove(event);
	}
}
