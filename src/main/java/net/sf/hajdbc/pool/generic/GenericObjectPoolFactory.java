package net.sf.hajdbc.pool.generic;

import java.util.NoSuchElementException;

import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggerFactory;
import net.sf.hajdbc.pool.Pool;
import net.sf.hajdbc.pool.PoolFactory;
import net.sf.hajdbc.pool.PoolProvider;

import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;

/**
 * Adapter for a <a href="http://commons.apache.org/pool">commons-pool</a> GenericObjectPool.
 * 
 * @author Paul Ferraro
 */
public class GenericObjectPoolFactory implements PoolFactory
{
	static final Logger logger = LoggerFactory.getLogger(GenericObjectPoolFactory.class);
	
	private final GenericObjectPool.Config config;
	
	public GenericObjectPoolFactory(GenericObjectPool.Config config)
	{
		this.config = config;
	}
	
	@Override
	public <T, E extends Exception> Pool<T, E> createPool(final PoolProvider<T, E> provider)
	{
		PoolableObjectFactory<T> factory = new PoolableObjectFactory<T>()
		{
			@Override
			public void destroyObject(T object)
			{
				provider.close(object);
			}

			@Override
			public T makeObject() throws Exception
			{
				return provider.create();
			}

			@Override
			public boolean validateObject(T object)
			{
				return provider.isValid(object);
			}
			
			@Override
			public void activateObject(T object)
			{
			}

			@Override
			public void passivateObject(T object)
			{
			}
		};

		final ObjectPool<T> pool = new GenericObjectPool<T>(factory, this.config);
		
		return new Pool<T, E>()
		{
			@Override
			public void close()
			{
				try
				{
					pool.close();
				}
				catch (Exception e)
				{
					logger.log(Level.WARN, e, e.getMessage());
				}
			}

			@Override
			public void release(T item)
			{
				try
				{
					pool.returnObject(item);
				}
				catch (Exception e)
				{
					logger.log(Level.WARN, e, e.getMessage());
				}
			}

			@Override
			public T take() throws E
			{
				try
				{
					return pool.borrowObject();
				}
				catch (NoSuchElementException e)
				{
					return provider.create();
				}
				catch (IllegalStateException e)
				{
					throw e;
				}
				catch (Exception e)
				{
					throw provider.getExceptionClass().cast(e);
				}
			}
		};
	}
}
