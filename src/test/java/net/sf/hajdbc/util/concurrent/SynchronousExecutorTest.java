/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Assert;

import org.junit.Test;

/**
 * @author Paul Ferraro
 *
 */
public class SynchronousExecutorTest
{
	@Test
	public void test() throws InterruptedException, ExecutionException
	{
		this.test(Arrays.asList(100, 1, 1), false);
	}

	@Test
	public void reverse() throws InterruptedException, ExecutionException
	{
		this.test(Arrays.asList(1, 100, 100), true);
	}

	public void test(List<Integer> sleeps, boolean reverse) throws InterruptedException, ExecutionException
	{
		ExecutorService service = Executors.newCachedThreadPool();
		try
		{
			List<Task> tasks = new ArrayList<Task>(sleeps.size());
			List<Integer> order = new CopyOnWriteArrayList<Integer>();
			List<Integer> expected = new ArrayList<Integer>(sleeps.size());
			for (int i = 0; i < sleeps.size(); ++i)
			{
				tasks.add(new Task(i, sleeps.get(i), order));
				expected.add(i);
			}
			
			List<Future<Integer>> futures = new SynchronousExecutor(service, reverse).invokeAll(tasks);
			
			List<Integer> results = new ArrayList<Integer>(tasks.size());
			for (Future<Integer> future: futures)
			{
				results.add(future.get());
			}
			
			Assert.assertEquals(expected, results);
			
			// Make sure 1st task finished first, or last if reversed
			Assert.assertEquals(0, order.get(reverse ? 2 : 0).intValue());
		}
		finally
		{
			service.shutdown();
		}
	}
	
	private class Task implements Callable<Integer>
	{
		private final int index;
		private final long sleep;
		private final List<Integer> order;
		
		Task(int index, long sleep, List<Integer> order)
		{
			this.index = index;
			this.sleep = sleep;
			this.order = order;
		}

		@Override
		public Integer call() throws Exception
		{
			try
			{
				Thread.sleep(this.sleep);
			}
			catch (InterruptedException e)
			{
				Thread.currentThread().interrupt();
			}
			this.order.add(this.index);
			return this.index;
		}
	}
}
