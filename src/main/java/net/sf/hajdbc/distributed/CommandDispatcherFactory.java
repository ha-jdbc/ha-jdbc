package net.sf.hajdbc.distributed;

public interface CommandDispatcherFactory
{
	<C> CommandDispatcher<C> createCommandDispatcher(String transportId, C context, Stateful stateful, MembershipListener membershipListener) throws Exception;
}
