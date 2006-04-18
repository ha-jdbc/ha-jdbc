/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2006 Paul Ferraro
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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Executor service that executes tasks in the caller thread.
 * 
 * @author Paul Ferraro
 */
public class SynchronousExecutor implements ExecutorService
{
	private boolean shutdown = false;
	
	/**
	 * @see java.util.concurrent.ExecutorService#shutdown()
	 */
	public void shutdown()
	{
		this.shutdown = true;
	}

	/**
	 * @see java.util.concurrent.ExecutorService#shutdownNow()
	 */
	public List<Runnable> shutdownNow()
	{
		this.shutdown();
		
		return Collections.emptyList();
	}

	/**
	 * @see java.util.concurrent.ExecutorService#isShutdown()
	 */
	public boolean isShutdown()
	{
		return this.shutdown;
	}

	/**
	 * @see java.util.concurrent.ExecutorService#isTerminated()
	 */
	public boolean isTerminated()
	{
		return this.isShutdown();
	}

	/**
	 * @see java.util.concurrent.ExecutorService#awaitTermination(long, java.util.concurrent.TimeUnit)
	 */
	public boolean awaitTermination(long timeout, TimeUnit unit)
	{
		return true;
	}

	/**
	 * @see java.util.concurrent.ExecutorService#submit(java.util.concurrent.Callable)
	 */
	public <T> Future<T> submit(Callable<T> task)
	{
		return new SynchronousFuture<T>(task);
	}

	/**
	 * @see java.util.concurrent.ExecutorService#submit(java.lang.Runnable, Object)
	 */
	public <T> Future<T> submit(Runnable task, T result)
	{
		return new SynchronousFuture<T>(new CallableTask<T>(task, result));
	}

	/**
	 * @see java.util.concurrent.ExecutorService#submit(java.lang.Runnable)
	 */
	public Future<?> submit(Runnable task)
	{
		return new SynchronousFuture<Void>(new CallableTask<Void>(task, null));
	}

	/**
	 * @see java.util.concurrent.ExecutorService#invokeAll(java.util.Collection)
	 */
	public <T> List<Future<T>> invokeAll(Collection<Callable<T>> tasks)
	{
		List<Future<T>> futureList = new ArrayList<Future<T>>(tasks.size());
		
		for (Callable<T> task: tasks)
		{
			futureList.add(new SynchronousFuture<T>(task));
		}
		
		return futureList;
	}

	/**
	 * @see java.util.concurrent.ExecutorService#invokeAll(java.util.Collection, long, java.util.concurrent.TimeUnit)
	 */
	public <T> List<Future<T>> invokeAll(Collection<Callable<T>> tasks, long timeout, TimeUnit unit)
	{
		return this.invokeAll(tasks);
	}

	/**
	 * @see java.util.concurrent.ExecutorService#invokeAny(java.util.Collection)
	 */
	public <T> T invokeAny(Collection<Callable<T>> tasks) throws ExecutionException
	{
		Throwable exception = null;
		
		for (Callable<T> task: tasks)
		{
			try
			{
				return task.call();
			}
			catch (Throwable e)
			{
				exception = e;
			}
		}
		
		throw new ExecutionException(exception);
	}

	/**
	 * @see java.util.concurrent.ExecutorService#invokeAny(java.util.Collection, long, java.util.concurrent.TimeUnit)
	 */
	public <T> T invokeAny(Collection<Callable<T>> tasks, long timeout, TimeUnit unit) throws ExecutionException
	{
		return this.invokeAny(tasks);
	}

	/**
	 * @see java.util.concurrent.Executor#execute(java.lang.Runnable)
	 */
	public void execute(Runnable task)
	{
		task.run();
	}
	
	private class SynchronousFuture<T> implements Future<T>
	{
		private T result = null;
		private Throwable exception = null;

		public SynchronousFuture(Callable<T> task)
		{
			try
			{
				this.result = task.call();
			}
			catch (Exception e)
			{
				this.exception = e;
			}
		}
		
		/**
		 * @see java.util.concurrent.Future#cancel(boolean)
		 */
		public boolean cancel(boolean mayInterruptIfRunning)
		{
			return false;
		}

		/**
		 * @see java.util.concurrent.Future#isCancelled()
		 */
		public boolean isCancelled()
		{
			return false;
		}

		/**
		 * @see java.util.concurrent.Future#isDone()
		 */
		public boolean isDone()
		{
			return true;
		}

		/**
		 * @see java.util.concurrent.Future#get()
		 */
		public T get() throws ExecutionException
		{
			if (this.exception != null)
			{
				throw new ExecutionException(this.exception);
			}
			
			return this.result;
		}

		/**
		 * @see java.util.concurrent.Future#get(long, java.util.concurrent.TimeUnit)
		 */
		public T get(long timeout, TimeUnit unit) throws ExecutionException
		{
			return this.get();
		}			
	}
	
	private class CallableTask<T> implements Callable<T>
	{
		private Runnable task;
		private T result;
		
		public CallableTask(Runnable task, T result)
		{
			this.task = task;
			this.result = result;
		}
		
		/**
		 * @see java.util.concurrent.Callable#call()
		 */
		public T call() throws Exception
		{
			this.task.run();
			
			return this.result;
		}		
	}
}
