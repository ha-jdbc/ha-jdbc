/*
 * HA-JDBC: High-Availablity JDBC
 * Copyright 2010 Paul Ferraro
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.sf.hajdbc.util.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.ExceptionFactory;
import net.sf.hajdbc.ExceptionType;
import net.sf.hajdbc.Lifecycle;
import net.sf.hajdbc.durability.Durability.Phase;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Paul Ferraro
 */
public class RegistryTest
{
	@Test
	public void simple() throws InterruptedException
	{
		SimpleObject expected = new SimpleObject(0);
		
		Registry<Void, SimpleObject, Void, Exception> registry = new LifecycleRegistry<Void, SimpleObject, Void, Exception>(new Factory(expected, 1), new ReferenceRegistryStoreFactory(), new SimpleExceptionFactory());
		
		int count = 100;
		List<Callable<SimpleObject>> tasks = new ArrayList<Callable<SimpleObject>>(count);
		for (int i = 0; i < count; ++i)
		{
			tasks.add(new RegistryTask(registry));
		}
		
		ExecutorService executor = Executors.newFixedThreadPool(count);
		
		try
		{
			List<Future<SimpleObject>> futures = executor.invokeAll(tasks);
			
			for (Future<SimpleObject> future: futures)
			{
				ExecutionException exception = null;
				
				try
				{
					Assert.assertSame(expected, future.get());
				}
				catch (ExecutionException e)
				{
					exception = e;
				}
				Assert.assertNull(exception);
			}
		}
		finally
		{
			executor.shutdown();
		}
	}

	@Test
	public void timeout() throws InterruptedException
	{
		SimpleObject expected = new SimpleObject(4);
		
		Registry<Void, SimpleObject, Void, Exception> registry = new LifecycleRegistry<Void, SimpleObject, Void, Exception>(new Factory(expected, 1), new ReferenceRegistryStoreFactory(), new SimpleExceptionFactory());
		
		int count = 100;
		List<Callable<SimpleObject>> tasks = new ArrayList<Callable<SimpleObject>>(count);
		for (int i = 0; i < count; ++i)
		{
			tasks.add(new RegistryTask(registry));
		}
		
		ExecutorService executor = Executors.newFixedThreadPool(count);
		
		try
		{
			List<Future<SimpleObject>> futures = executor.invokeAll(tasks);
			
			int fail = 0;
			
			// Only 1 of these will succeed, the rest will timeout.
			for (Future<SimpleObject> future: futures)
			{
				try
				{
					Assert.assertSame(expected, future.get());
				}
				catch (ExecutionException e)
				{
					Assert.assertEquals(TimeoutException.class, e.getCause().getClass());
					
					fail += 1;
				}
			}
	
			Assert.assertEquals(count - 1, fail);
			
			// All should succeed now
			List<Future<SimpleObject>> moreFutures = executor.invokeAll(tasks);
			
			for (Future<SimpleObject> future: moreFutures)
			{
				Exception exception = null;
				try
				{
					Assert.assertSame(expected, future.get());
				}
				catch (ExecutionException e)
				{
					exception = e;
				}
				Assert.assertNull(exception);
			}
		}
		finally
		{
			executor.shutdown();
		}
	}
	
	@Test
	public void retry() throws Exception
	{
		SimpleObject expected = new SimpleObject(0);
		
		Registry<Void, SimpleObject, Void, Exception> registry = new LifecycleRegistry<Void, SimpleObject, Void, Exception>(new Factory(expected, 1), new ReferenceRegistryStoreFactory(), new SimpleExceptionFactory());
		
		// Setup start() to fail
		expected.start();
		
		SimpleObject object = null;
		
		try
		{
			object = registry.get(null, null);
		}
		catch (Exception e)
		{
			Assert.assertEquals(IllegalStateException.class, e.getClass());
		}
		Assert.assertNull(object);
		
		// Setup start() to succeed
		expected.stop();

		Exception exception = null;
		try
		{
			Assert.assertSame(expected, registry.get(null, null));
		}
		catch (Exception e)
		{
			exception = e;
		}
		Assert.assertNull(exception);
	}
	
	class RegistryTask implements Callable<SimpleObject>
	{
		private final Registry<Void, SimpleObject, Void, Exception> registry;
		
		RegistryTask(Registry<Void, SimpleObject, Void, Exception> registry)
		{
			this.registry = registry;
		}

		@Override
		public SimpleObject call() throws Exception
		{
			return this.registry.get(null, null);
		}
	}
	
	class SimpleObject implements Lifecycle
	{
		private final AtomicBoolean started = new AtomicBoolean(false);
		private final int startupDuration;
		
		SimpleObject(int startupDuration)
		{
			this.startupDuration = startupDuration;
		}
		
		/**
		 * {@inheritDoc}
		 * @see net.sf.hajdbc.Lifecycle#start()
		 */
		@Override
		public void start() throws Exception
		{
			if (this.startupDuration > 0)
			{
				Thread.sleep(TimeUnit.SECONDS.toMillis(this.startupDuration));
			}
			
			if (!this.started.compareAndSet(false, true))
			{
				throw new IllegalStateException();
			}
		}

		/**
		 * {@inheritDoc}
		 * @see net.sf.hajdbc.Lifecycle#stop()
		 */
		@Override
		public void stop()
		{
			this.started.set(false);
		}
	}
	
	private class Factory implements LifecycleRegistry.Factory<Void, SimpleObject, Void, Exception>
	{
		private final SimpleObject object;
		private final int seconds;
		
		Factory(SimpleObject object, int seconds)
		{
			this.object = object;
			this.seconds = seconds;
		}
		
		@Override
		public SimpleObject create(Void key, Void context) throws Exception
		{
			return this.object;
		}

		@Override
		public long getTimeout()
		{
			return this.seconds;
		}

		@Override
		public TimeUnit getTimeoutUnit()
		{
			return TimeUnit.SECONDS;
		}
	}
	
	class SimpleExceptionFactory implements ExceptionFactory<Exception>
	{
		/**
		 * {@inheritDoc}
		 * @see net.sf.hajdbc.ExceptionFactory#createException(java.lang.Throwable)
		 */
		@Override
		public Exception createException(Throwable e)
		{
			return (Exception) e;
		}

		/**
		 * {@inheritDoc}
		 * @see net.sf.hajdbc.ExceptionFactory#createException(java.lang.String)
		 */
		@Override
		public Exception createException(String message)
		{
			return new Exception(message);
		}

		/**
		 * {@inheritDoc}
		 * @see net.sf.hajdbc.ExceptionFactory#equals(java.lang.Exception, java.lang.Exception)
		 */
		@Override
		public boolean equals(Exception exception1, Exception exception2)
		{
			return false;
		}

		/**
		 * {@inheritDoc}
		 * @see net.sf.hajdbc.ExceptionFactory#indicatesFailure(java.lang.Exception, net.sf.hajdbc.Dialect)
		 */
		@Override
		public boolean indicatesFailure(Exception exception, Dialect dialect)
		{
			return false;
		}

		/**
		 * {@inheritDoc}
		 * @see net.sf.hajdbc.ExceptionFactory#getType()
		 */
		@Override
		public ExceptionType getType()
		{
			return null;
		}

		/**
		 * {@inheritDoc}
		 * @see net.sf.hajdbc.ExceptionFactory#correctHeuristic(java.lang.Exception, net.sf.hajdbc.durability.Durability.Phase)
		 */
		@Override
		public boolean correctHeuristic(Exception exception, Phase phase)
		{
			return false;
		}
	}
}
