package net.sf.hajdbc.distributed.jgroups;

import net.sf.hajdbc.distributed.CommandDispatcher;
import net.sf.hajdbc.distributed.MembershipListener;
import net.sf.hajdbc.distributed.Stateful;

public abstract class AbstractChannelProvider implements ChannelProvider
{
	/**
	 * {@inheritDoc}
	 * @throws Exception 
	 * @see net.sf.hajdbc.distributed.CommandDispatcherFactory#createCommandDispatcher(java.lang.String, java.lang.Object, net.sf.hajdbc.distributed.Stateful, net.sf.hajdbc.distributed.MembershipListener)
	 */
	@Override
	public <C> CommandDispatcher<C> createCommandDispatcher(String channelName, C context, Stateful stateful, MembershipListener membershipListener) throws Exception
	{
		return new ChannelCommandDispatcher<C>(channelName, this, context, stateful, membershipListener);
	}
}
