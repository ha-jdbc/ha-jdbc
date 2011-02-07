package net.sf.hajdbc.invocation;

import java.util.SortedMap;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.sql.SQLProxy;

public enum InvocationStrategyEnum implements InvocationStrategy
{
	INVOKE_ON_ALL(new InvokeOnAllInvocationStrategy()),
	INVOKE_ON_ANY(new InvokeOnAnyInvocationStrategy()),
	INVOKE_ON_EXISTING(new InvokeOnExistingInvocationStrategy()),
	INVOKE_ON_MASTER(new InvokeOnMasterInvocationStrategy()),
	INVOKE_ON_NEXT(new InvokeOnNextInvocationStrategy()),
	TRANSACTION_INVOKE_ON_ALL(new TransactionInvokeOnAllInvocationStrategy(false)),
	END_TRANSACTION_INVOKE_ON_ALL(new TransactionInvokeOnAllInvocationStrategy(true));

	private final InvocationStrategy strategy;
	
	private InvocationStrategyEnum(InvocationStrategy strategy)
	{
		this.strategy = strategy;
	}
	
	@Override
	public <Z, D extends Database<Z>, T, R, E extends Exception> SortedMap<D, R> invoke(SQLProxy<Z, D, T, E> proxy, Invoker<Z, D, T, R, E> invoker) throws E
	{
		return this.strategy.invoke(proxy, invoker);
	}
}
