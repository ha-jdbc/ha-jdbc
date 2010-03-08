package net.sf.hajdbc.pool;

public abstract class AbstractPoolProvider<T, E extends Exception> implements PoolProvider<T, E>
{
	private final Class<T> providedClass;
	private final Class<E> exceptionClass;
	
	protected AbstractPoolProvider(Class<T> providedClass, Class<E> exceptionClass)
	{
		this.providedClass = providedClass;
		this.exceptionClass = exceptionClass;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.pool.PoolProvider#getExceptionClass()
	 */
	@Override
	public Class<E> getExceptionClass()
	{
		return this.exceptionClass;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.pool.PoolProvider#getProvidedClass()
	 */
	@Override
	public Class<T> getProvidedClass()
	{
		return this.providedClass;
	}
}
