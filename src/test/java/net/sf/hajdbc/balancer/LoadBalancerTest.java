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

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import net.sf.hajdbc.MockDatabase;

import org.junit.Assert;


/**
 * @author Paul Ferraro
 */
public class LoadBalancerTest extends AbstractBalancerTest
{
	public LoadBalancerTest()
	{
		super(BalancerFactoryEnum.LOAD);
	}
	
	@Override
	public void next(Balancer<Void, MockDatabase> balancer)
	{
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
}
