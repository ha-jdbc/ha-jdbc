package net.sf.hajdbc.sql;

import java.lang.reflect.InvocationHandler;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.Invoker;

public class ConnectionInvocationHandlerFactory<Z, D extends Database<Z>, P> implements InvocationHandlerFactory<Z, D, P, Connection, SQLException>
{
	private final TransactionContext<Z, D> context;

	public ConnectionInvocationHandlerFactory(TransactionContext<Z, D> context)
	{
		this.context = context;
	}
	
	@Override
	public InvocationHandler createInvocationHandler(P parent, SQLProxy<Z, D, P, SQLException> proxy, Invoker<Z, D, P, Connection, SQLException> invoker, Map<D, Connection> objects)
	{
		return new ConnectionInvocationHandler<Z, D, P>(parent, proxy, invoker, objects, this.context);
	}

	@Override
	public Class<Connection> getTargetClass()
	{
		return Connection.class;
	}
}
