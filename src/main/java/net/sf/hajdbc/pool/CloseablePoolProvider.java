package net.sf.hajdbc.pool;

import java.io.Closeable;
import java.io.IOException;

import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.LoggerFactory;

public abstract class CloseablePoolProvider<T extends Closeable, E extends Exception> extends AbstractPoolProvider<T, E>
{
	protected CloseablePoolProvider(Class<T> providedClass, Class<E> exceptionClass)
	{
		super(providedClass, exceptionClass);
	}

	@Override
	public void close(T object)
	{
		try
		{
			object.close();
		}
		catch (IOException e)
		{
			LoggerFactory.getLogger(this.getClass()).log(Level.WARN, e);
		}
	}
}
