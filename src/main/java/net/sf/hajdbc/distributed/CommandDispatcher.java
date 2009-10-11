package net.sf.hajdbc.distributed;

import java.util.Map;

import net.sf.hajdbc.Lifecycle;

public interface CommandDispatcher<C> extends Lifecycle
{
	<R> Map<Member, R> executeAll(Command<R, C> command);

	<R> R executeCoordinator(Command<R, C> command);

	boolean isCoordinator();

	Member getLocal();
}