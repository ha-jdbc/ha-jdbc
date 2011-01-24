/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2007 Paul Ferraro
 * 
 * This library is free software; you can redistribute it and/or modify it 
 * under the terms of the GNU Lesser General Public License as published by the 
 * Free Software Foundation; either version 2.1 of the License, or (at your 
 * option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License 
 * for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation, 
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 * 
 * Contact: ferraro@users.sourceforge.net
 */
package net.sf.hajdbc.util.concurrent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Executor service that executes tasks in the caller thread.
 * 
 * @author Paul Ferraro
 */
public class SynchronousExecutor implements ExecutorService
{
	private volatile boolean shutdown;
	
	private final boolean reverse;
	
	public SynchronousExecutor()
	{
		this(false);
	}
	
	public SynchronousExecutor(boolean reverse)
	{
		this.reverse = reverse;
	}
	
	/**
	 * @see java.util.concurrent.ExecutorService#awaitTermination(long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public boolean awaitTermination(long time, TimeUnit unit)
	{
		return true;
	}

	/**
	 * @see java.util.concurrent.ExecutorService#isShutdown()
	 */
	@Override
	public boolean isShutdown()
	{
		return this.shutdown;
	}

	/**
	 * @see java.util.concurrent.ExecutorService#isTerminated()
	 */
	@Override
	public boolean isTerminated()
	{
		return this.shutdown;
	}

	/**
	 * @see java.util.concurrent.ExecutorService#shutdown()
	 */
	@Override
	public void shutdown()
	{
		this.shutdown = true;
	}

	/**
	 * @see java.util.concurrent.ExecutorService#shutdownNow()
	 */
	@Override
	public List<Runnable> shutdownNow()
	{
		this.shutdown();
		
		return Collections.emptyList();
	}

	/**
	 * @see java.util.concurrent.Executor#execute(java.lang.Runnable)
	 */
	@Override
	public void execute(Runnable task)
	{
		task.run();
	}

	/**
	 * @see java.util.concurrent.ExecutorService#submit(java.util.concurrent.Callable)
	 */
	@Override
	public <T> Future<T> submit(Callable<T> task)
	{
		return new SynchronousFuture<T>(task);
	}

	/**
	 * @see java.util.concurrent.ExecutorService#submit(java.lang.Runnable, java.lang.Object)
	 */
	@Override
	public <T> Future<T> submit(Runnable task, T result)
	{
		return new SynchronousFuture<T>(Executors.callable(task, result));
	}

	/**
	 * @see java.util.concurrent.ExecutorService#submit(java.lang.Runnable)
	 */
	@Override
	public Future<?> submit(Runnable task)
	{
		return new SynchronousFuture<Object>(Executors.callable(task));
	}

	/**
	 * @see java.util.concurrent.ExecutorService#invokeAll(java.util.Collection)
	 */
	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException
	{
		if (tasks.isEmpty()) return Collections.emptyList();

		List<Callable<T>> taskList = new ArrayList<Callable<T>>(tasks);
		
		if (this.reverse)
		{
			Collections.reverse(taskList);
		}
		
		List<Future<T>> futureList = new ArrayList<Future<T>>(tasks.size());
		
		for (Callable<T> task: taskList)
		{
			futureList.add(this.submit(task));
		}
		
		if (this.reverse)
		{
			Collections.reverse(futureList);
		}
		
		return futureList;
	}

	/**
	 * @see java.util.concurrent.ExecutorService#invokeAll(java.util.Collection, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException
	{
		return this.invokeAll(tasks);
	}

	/**
	 * @see java.util.concurrent.ExecutorService#invokeAny(java.util.Collection)
	 */
	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException
	{
		if (tasks.isEmpty()) throw new IllegalArgumentException();

		return this.invokeAll(tasks).get(0).get();
	}

	/**
	 * @see java.util.concurrent.ExecutorService#invokeAny(java.util.Collection, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
	{
		return this.invokeAny(tasks);
	}
	
	/**
	 * Light-weight future implementation for synchronously executed tasks.
	 * @param <T>
	 */
	static class SynchronousFuture<T> implements Future<T>
	{
		private T result;
		private ExecutionException exception;
		
		SynchronousFuture(Callable<T> task)
		{
			try
			{
				this.result = task.call();
			}
			catch (Throwable e)
			{
				this.exception = new ExecutionException(e);
			}
		}
		
		/**
		 * {@inheritDoc}
		 * @see java.util.concurrent.Future#cancel(boolean)
		 */
		@Override
		public boolean cancel(boolean mayInterruptIfRunning)
		{
			return false;
		}

		/**
		 * {@inheritDoc}
		 * @see java.util.concurrent.Future#get()
		 */
		@Override
		public T get() throws ExecutionException
		{
			if (this.exception != null) throw this.exception;
			
			return this.result;
		}

		/**
		 * {@inheritDoc}
		 * @see java.util.concurrent.Future#get(long, java.util.concurrent.TimeUnit)
		 */
		@Override
		public T get(long time, TimeUnit unit) throws ExecutionException
		{
			return this.get();
		}

		/**
		 * {@inheritDoc}
		 * @see java.util.concurrent.Future#isCancelled()
		 */
		@Override
		public boolean isCancelled()
		{
			return false;
		}

		/**
		 * {@inheritDoc}
		 * @see java.util.concurrent.Future#isDone()
		 */
		@Override
		public boolean isDone()
		{
			return true;
		}
		
		boolean isSuccessful()
		{
			return this.exception == null;
		}
	}
}
