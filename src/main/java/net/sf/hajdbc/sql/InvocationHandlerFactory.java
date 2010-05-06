package net.sf.hajdbc.sql;

import java.lang.reflect.InvocationHandler;
import java.util.Map;

import net.sf.hajdbc.Database;

public interface InvocationHandlerFactory<Z, D extends Database<Z>, P, T, E extends Exception>
{
	InvocationHandler createInvocationHandler(P parent, SQLProxy<Z, D, P, E> proxy, Invoker<Z, D, P, T, E> invoker, Map<D, T> objects) throws E;
	
	Class<T> getTargetClass();
}
