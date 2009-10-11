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
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Executor service that executes tasks in the caller thread.
 * 
 * @author Paul Ferraro
 */
public class SynchronousExecutor extends AbstractExecutorService
{
	private final ExecutorService executor;
	
	public SynchronousExecutor(ExecutorService executor)
	{
		this.executor = executor;
	}
	
	/**
	 * @see java.util.concurrent.ExecutorService#awaitTermination(long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public boolean awaitTermination(long time, TimeUnit unit) throws InterruptedException
	{
		return this.executor.awaitTermination(time, unit);
	}

	/**
	 * @see java.util.concurrent.ExecutorService#isShutdown()
	 */
	@Override
	public boolean isShutdown()
	{
		return this.executor.isShutdown();
	}

	/**
	 * @see java.util.concurrent.ExecutorService#isTerminated()
	 */
	@Override
	public boolean isTerminated()
	{
		return this.executor.isTerminated();
	}

	/**
	 * @see java.util.concurrent.ExecutorService#shutdown()
	 */
	@Override
	public void shutdown()
	{
		this.executor.shutdown();
	}

	/**
	 * @see java.util.concurrent.ExecutorService#shutdownNow()
	 */
	@Override
	public List<Runnable> shutdownNow()
	{
		return this.executor.shutdownNow();
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
	 * Executes the specified tasks serially, until the first successful result, then the remainder using the executor with which this executor was created.
	 * {@inheritDoc}
	 * @see java.util.concurrent.AbstractExecutorService#invokeAll(java.util.Collection)
	 */
	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException
	{
		if (tasks.isEmpty()) return Collections.emptyList();

		int remaining = tasks.size();
		List<Future<T>> futureList = new ArrayList<Future<T>>(remaining);
		
		boolean synchronous = true;
		
		for (Callable<T> task: tasks)
		{
			remaining -= 1;
			
			if (synchronous)
			{
				SynchronousFuture<T> future = new SynchronousFuture<T>(task);
				
				futureList.add(future);

				// Execute remaining tasks in parallel, if there are multiple
				if (future.isSuccessful() && (remaining > 2))
				{
					synchronous = false;
				}
			}
			else
			{
				futureList.add(this.executor.submit(task));
			}
		}

		try
		{
			// Wait until all tasks have finished
			for (Future<T> future: futureList)
			{
				if (!future.isDone())
				{
					try
					{
						future.get();
					}
					catch (ExecutionException e)
					{
						// Ignore
					}
					catch (CancellationException e)
					{
						// Ignore
					}
				}
			}
			
			return futureList;
		}
		finally
		{
			// If interrupted, cancel any unfinished tasks
			for (Future<T> future: futureList)
			{
				if (!future.isDone())
				{
					future.cancel(true);
				}
			}
		}
	}

	/**
	 * {@inheritDoc}
	 * @see java.util.concurrent.AbstractExecutorService#invokeAll(java.util.Collection, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException
	{
		if (tasks.isEmpty()) return Collections.emptyList();

		int remaining = tasks.size();
		List<Future<T>> futureList = new ArrayList<Future<T>>(remaining);
		
		boolean synchronous = true;
		
		for (Callable<T> task: tasks)
		{
			remaining -= 1;
			
			if (synchronous)
			{
				SynchronousFuture<T> future = new SynchronousFuture<T>(task);
				
				futureList.add(future);

				// Execute remaining tasks in parallel, if there are multiple
				if (future.isSuccessful() && (remaining > 1))
				{
					synchronous = false;
				}
			}
			else
			{
				futureList.add(this.executor.submit(task));
			}
		}

		boolean interrupted = true;
		
		try
		{
			// Wait until all tasks have finished
			for (Future<T> future: futureList)
			{
				if (!future.isDone())
				{
					try
					{
						future.get();
					}
					catch (ExecutionException e)
					{
						// Ignore
					}
					catch (CancellationException e)
					{
						// Ignore
					}
				}
			}

			interrupted = false;
			
			return futureList;
		}
		finally
		{
			if (interrupted)
			{
				// If interrupted, cancel any incomplete tasks
				for (Future<T> future: futureList)
				{
					if (!future.isDone())
					{
						future.cancel(true);
					}
				}
			}
		}
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
			catch (Exception e)
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
