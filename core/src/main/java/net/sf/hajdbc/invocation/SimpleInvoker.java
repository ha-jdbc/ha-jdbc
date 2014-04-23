package net.sf.hajdbc.invocation;

import java.lang.reflect.Method;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.ExceptionFactory;
import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggerFactory;
import net.sf.hajdbc.util.reflect.Methods;

public class SimpleInvoker<Z, D extends Database<Z>, T, R, E extends Exception> implements Invoker<Z, D, T, R, E>
{
	private static final Logger logger = LoggerFactory.getLogger(SimpleInvoker.class);
	
	private final Method method;
	private final Object[] parameters;
	private final ExceptionFactory<E> exceptionFactory;
	
	/**
	 * @param method
	 * @param parameters
	 */
	public SimpleInvoker(Method method, Object[] parameters, ExceptionFactory<E> exceptionFactory)
	{
		this.method = method;
		this.parameters = parameters;
		this.exceptionFactory = exceptionFactory;
	}
	
	public Method getMethod()
	{
		return this.method;
	}
	
	public Object[] getParameters()
	{
		return this.parameters;
	}
	
	public ExceptionFactory<E> getExceptionFactory()
	{
		return this.exceptionFactory;
	}
	
	/**
	 * @see net.sf.hajdbc.invocation.Invoker#invoke(net.sf.hajdbc.Database, java.lang.Object)
	 */
	@Override
	public R invoke(D database, T object) throws E
	{
		logger.log(Level.TRACE, "Invoking {0} against {1}", this.method, database);
		return Methods.<R, E>invoke(this.method, this.exceptionFactory, object, this.parameters);
	}

	@Override
	public int hashCode()
	{
		return this.method.hashCode();
	}

	@Override
	public boolean equals(Object object)
	{
		if ((object != null) && (object instanceof SimpleInvoker))
		{
			@SuppressWarnings("unchecked")
			SimpleInvoker<Z, D, T, R, E> invoker = (SimpleInvoker<Z, D, T, R, E>) object;
			return this.method.equals(invoker.method);
		}
		return false;
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return this.method.toString();
	}
}
