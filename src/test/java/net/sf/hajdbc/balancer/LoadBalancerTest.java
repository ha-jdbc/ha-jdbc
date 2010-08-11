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
package net.sf.hajdbc.balancer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import net.sf.hajdbc.MockDatabase;
import net.sf.hajdbc.sql.Invoker;

import org.junit.Assert;
import org.junit.Test;


/**
 * @author Paul Ferraro
 */
public class LoadBalancerTest extends AbstractBalancerTest
{
	public LoadBalancerTest()
	{
		super(BalancerFactoryEnum.LOAD);
	}
	
	@Test
	public void nextEmptyBalancer()
	{
		Balancer<Void, MockDatabase> balancer = this.factory.createBalancer(Collections.<MockDatabase>emptySet());
		
		Assert.assertNull(balancer.next());
	}
	
	@Test
	public void nextZeroWeight()
	{
		Balancer<Void, MockDatabase> balancer = this.factory.createBalancer(Collections.singleton(this.databases[0]));
		
		Assert.assertSame(this.databases[0], balancer.next());
	}
	
	@Test
	public void nextSingleWeight()
	{
		Balancer<Void, MockDatabase> balancer = this.factory.createBalancer(new HashSet<MockDatabase>(Arrays.asList(this.databases[0], this.databases[1])));
		
		Assert.assertSame(this.databases[1], balancer.next());
		
		int count = 10;
		
		CountDownLatch latch = new CountDownLatch(count);
		WaitingInvoker invoker = new WaitingInvoker(latch);
		
		ExecutorService executor = Executors.newFixedThreadPool(count);
		List<Future<Void>> futures = new ArrayList<Future<Void>>(count);
		
		for (int i = 0; i < count; ++i)
		{
			futures.add(executor.submit(new InvocationTask(balancer, invoker, this.databases[1])));
		}
		
		try
		{
			// Ensure all invokers are started
			latch.await();
			
			// Should still use the same database, even under load
			Assert.assertSame(this.databases[1], balancer.next());
			
			// Allow invokers to continue
			synchronized (invoker)
			{
				invoker.notifyAll();
			}
			
			this.complete(futures);
			
			// Should still use the same database, after load
			Assert.assertSame(this.databases[1], balancer.next());
		}
		catch (InterruptedException e)
		{
			Thread.currentThread().interrupt();
		}
		finally
		{
			executor.shutdownNow();
		}
	}
	
	@Test
	public void nextMultipleWeights()
	{
		Balancer<Void, MockDatabase> balancer = this.factory.createBalancer(new HashSet<MockDatabase>(Arrays.asList(this.databases)));
		
		Assert.assertSame(this.databases[2], balancer.next());
		
		ExecutorService executor = Executors.newFixedThreadPool(3);
		
		CountDownLatch latch = new CountDownLatch(2);
		WaitingInvoker invoker1 = new WaitingInvoker(latch);
		
		Future<Void> future1 = executor.submit(new InvocationTask(balancer, invoker1, this.databases[2]));
		Future<Void> future2 = executor.submit(new InvocationTask(balancer, invoker1, this.databases[2]));
		
		try
		{
			latch.await();
			
			Assert.assertSame(this.databases[1], balancer.next());
			
			latch = new CountDownLatch(1);
			WaitingInvoker invoker2 = new WaitingInvoker(latch);
			
			Future<Void> future = executor.submit(new InvocationTask(balancer, invoker2, this.databases[1]));
			
			latch.await();
			
			Assert.assertSame(this.databases[2], balancer.next());
			
			synchronized (invoker2)
			{
				invoker2.notifyAll();
			}
			
			this.complete(Collections.singletonList(future));
			
			Assert.assertSame(this.databases[1], balancer.next());
			
			synchronized (invoker1)
			{
				invoker1.notifyAll();
			}
			
			this.complete(Collections.singletonList(future1));
			this.complete(Collections.singletonList(future2));
			
			Assert.assertSame(this.databases[2], balancer.next());
		}
		catch (InterruptedException e)
		{
			Thread.currentThread().interrupt();
		}
		finally
		{
			executor.shutdownNow();
		}
	}
	
	private void complete(List<Future<Void>> futures) throws InterruptedException
	{
		for (Future<Void> future: futures)
		{
			try
			{
				future.get();
			}
			catch (ExecutionException e)
			{
				Assert.fail(e.getCause().toString());
			}
		}
	}
	
	private class WaitingInvoker implements Invoker<Void, MockDatabase, Void, Void, Exception>
	{
		private final CountDownLatch latch;
		
		WaitingInvoker(CountDownLatch latch)
		{
			this.latch = latch;
		}
		
		@Override
		public Void invoke(MockDatabase database, Void object)
		{
			synchronized (this)
			{
				this.latch.countDown();
				
				try
				{
					this.wait();
				}
				catch (InterruptedException e)
				{
					Thread.currentThread().interrupt();
				}
			}
			return null;
		}
	}
	
	private static class InvocationTask implements Callable<Void>
	{
		private final Balancer<Void, MockDatabase> balancer;
		private final WaitingInvoker invoker;
		private final MockDatabase database;
		
		InvocationTask(Balancer<Void, MockDatabase> balancer, WaitingInvoker invoker, MockDatabase database)
		{
			this.balancer = balancer;
			this.invoker = invoker;
			this.database = database;
		}
		
		@Override
		public Void call() throws Exception
		{
			this.balancer.invoke(this.invoker, this.database, null);
			return null;
		}
	}
}
