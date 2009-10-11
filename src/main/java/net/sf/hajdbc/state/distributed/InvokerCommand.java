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
		Map<InvocationEvent, Map<D, InvokerEvent>> invokers = context.getRemoteInvokers(this.descriptor);

		InvokerEvent event = this.descriptor.getEvent();
		D database = context.getDatabaseCluster().getDatabase(event.getDatabaseId());
		
		synchronized (invokers)
		{
			Map<D, InvokerEvent> map = invokers.get(event);
			
			if (map != null)
			{
				map.put(database, event);
			}
		}
		
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.distributed.Command#marshalResult(java.lang.Object)
	 */
	@Override
	public Object marshalResult(Void result)
	{
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.distributed.Command#unmarshalResult(java.lang.Object)
	 */
	@Override
	public Void unmarshalResult(Object object)
	{
		return null;
	}
}
