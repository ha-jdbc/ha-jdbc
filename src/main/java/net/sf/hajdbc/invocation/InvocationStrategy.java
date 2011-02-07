package net.sf.hajdbc.invocation;

import java.util.SortedMap;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.sql.SQLProxy;

public interface InvocationStrategy
{
	/**
	 * Invoke the specified invoker against the specified proxy.
	 * @param proxy a JDBC object proxy
	 * @param invoker an invoker
	 * @return a result
	 * @throws Exception if the invocation fails
	 */
	<Z, D extends Database<Z>, T, R, E extends Exception> SortedMap<D, R> invoke(SQLProxy<Z, D, T, E> proxy, Invoker<Z, D, T, R, E> invoker) throws E;
}
