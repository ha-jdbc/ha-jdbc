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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import net.sf.hajdbc.MockDatabase;
import net.sf.hajdbc.invocation.Invoker;

import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Paul Ferraro
 */
public abstract class AbstractBalancerTest
{
	final BalancerFactory factory;
	final MockDatabase[] databases = new MockDatabase[] { new MockDatabase("0", 0), new MockDatabase("1", 1), new MockDatabase("2", 2) };
	
	AbstractBalancerTest(BalancerFactoryEnum factory)
	{
		this.factory = factory;
	}
	
	@Test
	public void next()
	{
		Balancer<Void, MockDatabase> balancer = this.factory.createBalancer(new HashSet<MockDatabase>(Arrays.asList(this.databases)));
		this.next(balancer);
	}
	
	protected abstract void next(Balancer<Void, MockDatabase> balancer);
	
	@Test
	public void nextEmptyBalancer()
	{
		Balancer<Void, MockDatabase> balancer = this.factory.createBalancer(Collections.<MockDatabase>emptySet());
		
		assertNull(balancer.next());
	}
	
	@Test
	public void nextZeroWeight()
	{
		Balancer<Void, MockDatabase> balancer = this.factory.createBalancer(Collections.singleton(this.databases[0]));
		
		assertSame(this.databases[0], balancer.next());
	}
	
	@Test
	public void nextSingleWeight()
	{
		Balancer<Void, MockDatabase> balancer = this.factory.createBalancer(new HashSet<MockDatabase>(Arrays.asList(this.databases[0], this.databases[1])));
		
		assertSame(this.databases[1], balancer.next());
		
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
			assertSame(this.databases[1], balancer.next());
			
			// Allow invokers to continue
			synchronized (invoker)
			{
				invoker.notifyAll();
			}
			
			this.complete(futures);
			
			// Should still use the same database, after load
			assertSame(this.databases[1], balancer.next());
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
	public void primary()
	{
		Balancer<Void, MockDatabase> balancer = this.factory.createBalancer(Collections.<MockDatabase>emptySet());

		assertNull(balancer.primary());
		
		balancer = this.factory.createBalancer(Collections.singleton(this.databases[0]));
		
		assertSame(this.databases[0], balancer.primary());

		balancer = this.factory.createBalancer(new HashSet<MockDatabase>(Arrays.asList(this.databases[0], this.databases[1])));
		
		assertSame(this.databases[0], balancer.primary());

		balancer = this.factory.createBalancer(new HashSet<MockDatabase>(Arrays.asList(this.databases)));
		
		assertSame(this.databases[0], balancer.primary());
	}

	@Test
	public void backups()
	{
		Balancer<Void, MockDatabase> balancer = this.factory.createBalancer(Collections.<MockDatabase>emptySet());

		Iterable<MockDatabase> result = balancer.backups();		
		assertNotNull(result);
		Iterator<MockDatabase> backups = result.iterator();
		assertFalse(backups.hasNext());
		
		balancer = this.factory.createBalancer(Collections.singleton(this.databases[0]));
		
		result = balancer.backups();
		assertNotNull(result);
		backups = result.iterator();
		assertFalse(backups.hasNext());

		balancer = this.factory.createBalancer(new HashSet<MockDatabase>(Arrays.asList(this.databases[0], this.databases[1])));
		
		result = balancer.backups();
		assertNotNull(result);
		backups = result.iterator();
		assertTrue(backups.hasNext());
		assertSame(this.databases[1], backups.next());
		assertFalse(backups.hasNext());
		
		balancer = this.factory.createBalancer(new HashSet<MockDatabase>(Arrays.asList(this.databases)));
		
		result = balancer.backups();
		assertNotNull(result);
		backups = result.iterator();
		assertTrue(backups.hasNext());
		assertSame(this.databases[1], backups.next());
		assertTrue(backups.hasNext());
		assertSame(this.databases[2], backups.next());
		assertFalse(backups.hasNext());
	}

	/**
	 * Test method for {@link net.sf.hajdbc.balancer.load.LoadBalancer#addAll(java.util.Collection)}.
	 */
	@Test
	public void addAll()
	{
		Collection<MockDatabase> databases = Arrays.asList(this.databases[1], this.databases[2]);
		
		Balancer<Void, MockDatabase> balancer = this.factory.createBalancer(Collections.<MockDatabase>emptySet());

		boolean result = balancer.addAll(databases);
		
		assertTrue(result);
		assertCollectionEquals(databases, balancer);
		
		balancer = this.factory.createBalancer(Collections.singleton(this.databases[0]));

		result = balancer.addAll(databases);
		
		assertTrue(result);
		assertCollectionEquals(Arrays.asList(this.databases), balancer);
		
		balancer = this.factory.createBalancer(new HashSet<MockDatabase>(Arrays.asList(this.databases[0], this.databases[1])));

		result = balancer.addAll(databases);
		
		assertTrue(result);
		assertCollectionEquals(Arrays.asList(this.databases), balancer);
		
		balancer = this.factory.createBalancer(new HashSet<MockDatabase>(Arrays.asList(this.databases)));

		result = balancer.addAll(databases);
		
		assertFalse(result);
		assertCollectionEquals(Arrays.asList(this.databases), balancer);
	}

	/**
	 * Test method for {@link net.sf.hajdbc.balancer.load.LoadBalancer#removeAll(java.util.Collection)}.
	 */
	@Test
	public void removeAll()
	{
		Collection<MockDatabase> databases = Arrays.asList(this.databases[1], this.databases[2]);
		
		Balancer<Void, MockDatabase> balancer = this.factory.createBalancer(Collections.<MockDatabase>emptySet());

		boolean result = balancer.removeAll(databases);
		
		assertFalse(result);
		assertEquals(Collections.<MockDatabase>emptySet(), balancer);
		
		balancer = this.factory.createBalancer(Collections.singleton(this.databases[0]));

		result = balancer.removeAll(databases);
		
		assertFalse(result);
		assertEquals(Collections.singleton(this.databases[0]), balancer);
		
		balancer = this.factory.createBalancer(new HashSet<MockDatabase>(Arrays.asList(this.databases[0], this.databases[1])));

		result = balancer.removeAll(databases);
		
		assertTrue(result);
		assertEquals(Collections.singleton(this.databases[0]), balancer);
		
		balancer = this.factory.createBalancer(new HashSet<MockDatabase>(Arrays.asList(this.databases)));

		result = balancer.removeAll(databases);
		
		assertTrue(result);
		assertEquals(Collections.singleton(this.databases[0]), balancer);
	}

	/**
	 * Test method for {@link net.sf.hajdbc.balancer.load.LoadBalancer#retainAll(java.util.Collection)}.
	 */
	@Test
	public void retainAll()
	{
		Collection<MockDatabase> databases = Arrays.asList(this.databases[1], this.databases[2]);
		
		Balancer<Void, MockDatabase> balancer = this.factory.createBalancer(Collections.<MockDatabase>emptySet());

		boolean result = balancer.retainAll(databases);
		
		assertFalse(result);
		assertCollectionEquals(Collections.<MockDatabase>emptySet(), balancer);
		
		balancer = this.factory.createBalancer(Collections.singleton(this.databases[0]));

		result = balancer.retainAll(databases);
		
		assertTrue(result);
		assertCollectionEquals(Collections.<MockDatabase>emptySet(), balancer);
		
		balancer = this.factory.createBalancer(new HashSet<MockDatabase>(Arrays.asList(this.databases[0], this.databases[1])));

		result = balancer.retainAll(databases);
		
		assertTrue(result);
		assertCollectionEquals(Collections.singleton(this.databases[1]), balancer);
		
		balancer = this.factory.createBalancer(new HashSet<MockDatabase>(Arrays.asList(this.databases)));

		result = balancer.retainAll(databases);
		
		assertTrue(result);
		assertCollectionEquals(databases, balancer);
	}
	
	/**
	 * Test method for {@link net.sf.hajdbc.balancer.load.LoadBalancer#clear()}.
	 */
	@Test
	public void clear()
	{
		Balancer<Void, MockDatabase> balancer = this.factory.createBalancer(Collections.<MockDatabase>emptySet());

		balancer.clear();
		assertEquals(Collections.<MockDatabase>emptySet(), balancer);
		
		balancer = this.factory.createBalancer(Collections.singleton(this.databases[0]));

		balancer.clear();
		assertEquals(Collections.<MockDatabase>emptySet(), balancer);

		balancer = this.factory.createBalancer(new HashSet<MockDatabase>(Arrays.asList(this.databases[0], this.databases[1])));
		
		balancer.clear();
		assertEquals(Collections.<MockDatabase>emptySet(), balancer);

		balancer = this.factory.createBalancer(new HashSet<MockDatabase>(Arrays.asList(this.databases)));
		
		balancer.clear();
		assertEquals(Collections.<MockDatabase>emptySet(), balancer);
	}

	/**
	 * Test method for {@link net.sf.hajdbc.balancer.load.LoadBalancer#remove(java.lang.Object)}.
	 */
	@Test
	public void remove()
	{
		Balancer<Void, MockDatabase> balancer = this.factory.createBalancer(Collections.<MockDatabase>emptySet());

		boolean result = balancer.remove(this.databases[1]);
		assertFalse(result);
		assertCollectionEquals(Collections.<MockDatabase>emptySet(), balancer);
		
		balancer = this.factory.createBalancer(Collections.singleton(this.databases[0]));

		result = balancer.remove(this.databases[1]);
		assertFalse(result);
		assertCollectionEquals(Collections.singleton(this.databases[0]), balancer);

		balancer = this.factory.createBalancer(new HashSet<MockDatabase>(Arrays.asList(this.databases[0], this.databases[1])));
		
		result = balancer.remove(this.databases[1]);
		assertTrue(result);
		assertCollectionEquals(Collections.singleton(this.databases[0]), balancer);

		balancer = this.factory.createBalancer(new HashSet<MockDatabase>(Arrays.asList(this.databases)));
		
		result = balancer.remove(this.databases[1]);
		assertTrue(result);
		assertCollectionEquals(Arrays.asList(this.databases[0], this.databases[2]), balancer);
	}

	/**
	 * Test method for {@link net.sf.hajdbc.balancer.load.LoadBalancer#add(net.sf.hajdbc.Database)}.
	 */
	@Test
	public void add()
	{
		Balancer<Void, MockDatabase> balancer = this.factory.createBalancer(Collections.<MockDatabase>emptySet());

		boolean result = balancer.add(this.databases[1]);
		assertTrue(result);
		assertCollectionEquals(Collections.singleton(this.databases[1]), balancer);
		
		balancer = this.factory.createBalancer(Collections.singleton(this.databases[0]));

		result = balancer.add(this.databases[1]);
		assertTrue(result);
		assertCollectionEquals(Arrays.asList(this.databases[0], this.databases[1]), balancer);

		balancer = this.factory.createBalancer(new HashSet<MockDatabase>(Arrays.asList(this.databases[0], this.databases[1])));
		
		result = balancer.add(this.databases[1]);
		assertFalse(result);
		assertCollectionEquals(Arrays.asList(this.databases[0], this.databases[1]), balancer);

		balancer = this.factory.createBalancer(new HashSet<MockDatabase>(Arrays.asList(this.databases)));
		
		result = balancer.add(this.databases[1]);
		assertFalse(result);
		assertCollectionEquals(Arrays.asList(this.databases), balancer);
	}

	/**
	 * Test method for {@link net.sf.hajdbc.balancer.load.LoadBalancer#invoke(net.sf.hajdbc.invocation.Invoker, net.sf.hajdbc.Database, java.lang.Object)}.
	 */
	@Test
	public void invoke() throws Exception
	{
		TestInvoker invoker = mock(TestInvoker.class);
		Object object = new Object();
		Object expected = new Object();
		Exception expectedException = new Exception();
		
		Balancer<Void, MockDatabase> balancer = this.factory.createBalancer(new HashSet<MockDatabase>(Arrays.asList(this.databases)));

		when(invoker.invoke(this.databases[0], object)).thenReturn(expected);
		
		Object result = null;
		Exception exception = null;
		try
		{
			result = balancer.invoke(invoker, this.databases[0], object);
		}
		catch (Exception e)
		{
			exception = e;
		}
		
		assertSame(expected, result);
		assertNull(exception);

		reset(invoker);
		
		when(invoker.invoke(this.databases[0], object)).thenThrow(expectedException);
		
		result = null;
		exception = null;
		
		try
		{
			result = balancer.invoke(invoker, this.databases[0], object);
		}
		catch (Exception e)
		{
			exception = e;
		}
		
		assertNull(result);
		assertSame(expectedException, exception);
	}

	interface TestInvoker extends Invoker<Void, MockDatabase, Object, Object, Exception> {}
	
	/**
	 * Test method for {@link net.sf.hajdbc.balancer.AbstractBalancer#iterator()}.
	 */
	@Test
	public void iterator()
	{
		Balancer<Void, MockDatabase> balancer = this.factory.createBalancer(Collections.<MockDatabase>emptySet());

		Iterator<MockDatabase> result = balancer.iterator();
		assertFalse(result.hasNext());
		
		balancer = this.factory.createBalancer(Collections.singleton(this.databases[0]));

		result = balancer.iterator();
		assertTrue(result.hasNext());
		assertEquals(this.databases[0], result.next());
		assertFalse(result.hasNext());

		balancer = this.factory.createBalancer(new HashSet<MockDatabase>(Arrays.asList(this.databases[0], this.databases[1])));
		
		result = balancer.iterator();
		assertTrue(result.hasNext());
		assertEquals(this.databases[0], result.next());
		assertTrue(result.hasNext());
		assertEquals(this.databases[1], result.next());
		assertFalse(result.hasNext());

		balancer = this.factory.createBalancer(new HashSet<MockDatabase>(Arrays.asList(this.databases)));
		
		result = balancer.iterator();
		assertTrue(result.hasNext());
		assertEquals(this.databases[0], result.next());
		assertTrue(result.hasNext());
		assertEquals(this.databases[1], result.next());
		assertTrue(result.hasNext());
		assertEquals(this.databases[2], result.next());
		assertFalse(result.hasNext());
	}

	/**
	 * Test method for {@link net.sf.hajdbc.balancer.AbstractBalancer#contains(java.lang.Object)}.
	 */
	@Test
	public void contains()
	{
		Balancer<Void, MockDatabase> balancer = this.factory.createBalancer(Collections.<MockDatabase>emptySet());

		assertFalse(balancer.contains(this.databases[0]));
		assertFalse(balancer.contains(this.databases[1]));
		assertFalse(balancer.contains(this.databases[2]));
		
		balancer = this.factory.createBalancer(Collections.singleton(this.databases[0]));

		assertTrue(balancer.contains(this.databases[0]));
		assertFalse(balancer.contains(this.databases[1]));
		assertFalse(balancer.contains(this.databases[2]));

		balancer = this.factory.createBalancer(new HashSet<MockDatabase>(Arrays.asList(this.databases[0], this.databases[1])));
		
		assertTrue(balancer.contains(this.databases[0]));
		assertTrue(balancer.contains(this.databases[1]));
		assertFalse(balancer.contains(this.databases[2]));

		balancer = this.factory.createBalancer(new HashSet<MockDatabase>(Arrays.asList(this.databases)));
		
		assertTrue(balancer.contains(this.databases[0]));
		assertTrue(balancer.contains(this.databases[1]));
		assertTrue(balancer.contains(this.databases[2]));
	}

	/**
	 * Test method for {@link net.sf.hajdbc.balancer.AbstractBalancer#containsAll(java.util.Collection)}.
	 */
	@Test
	public void containsAll()
	{
		Balancer<Void, MockDatabase> balancer = this.factory.createBalancer(Collections.<MockDatabase>emptySet());

		assertTrue(balancer.containsAll(Collections.emptyList()));
		assertFalse(balancer.containsAll(Collections.singletonList(this.databases[0])));
		assertFalse(balancer.containsAll(Arrays.asList(this.databases[0], this.databases[1])));
		assertFalse(balancer.containsAll(Arrays.asList(this.databases)));
		
		balancer = this.factory.createBalancer(Collections.singleton(this.databases[0]));

		assertTrue(balancer.containsAll(Collections.emptyList()));
		assertTrue(balancer.containsAll(Collections.singletonList(this.databases[0])));
		assertFalse(balancer.containsAll(Arrays.asList(this.databases[0], this.databases[1])));
		assertFalse(balancer.containsAll(Arrays.asList(this.databases)));

		balancer = this.factory.createBalancer(new HashSet<MockDatabase>(Arrays.asList(this.databases[0], this.databases[1])));
		
		assertTrue(balancer.containsAll(Collections.emptyList()));
		assertTrue(balancer.containsAll(Collections.singletonList(this.databases[0])));
		assertTrue(balancer.containsAll(Arrays.asList(this.databases[0], this.databases[1])));
		assertFalse(balancer.containsAll(Arrays.asList(this.databases)));

		balancer = this.factory.createBalancer(new HashSet<MockDatabase>(Arrays.asList(this.databases)));
		
		assertTrue(balancer.containsAll(Collections.emptyList()));
		assertTrue(balancer.containsAll(Collections.singletonList(this.databases[0])));
		assertTrue(balancer.containsAll(Arrays.asList(this.databases[0], this.databases[1])));
		assertTrue(balancer.containsAll(Arrays.asList(this.databases)));
	}

	/**
	 * Test method for {@link net.sf.hajdbc.balancer.AbstractBalancer#isEmpty()}.
	 */
	@Test
	public void isEmpty()
	{
		Balancer<Void, MockDatabase> balancer = this.factory.createBalancer(Collections.<MockDatabase>emptySet());

		assertTrue(balancer.isEmpty());
		
		balancer = this.factory.createBalancer(Collections.singleton(this.databases[0]));

		assertFalse(balancer.isEmpty());

		balancer = this.factory.createBalancer(new HashSet<MockDatabase>(Arrays.asList(this.databases[0], this.databases[1])));
		
		assertFalse(balancer.isEmpty());

		balancer = this.factory.createBalancer(new HashSet<MockDatabase>(Arrays.asList(this.databases)));
		
		assertFalse(balancer.isEmpty());
	}

	/**
	 * Test method for {@link net.sf.hajdbc.balancer.AbstractBalancer#size()}.
	 */
	@Test
	public void size()
	{
		Balancer<Void, MockDatabase> balancer = this.factory.createBalancer(Collections.<MockDatabase>emptySet());

		assertEquals(0, balancer.size());
		
		balancer = this.factory.createBalancer(Collections.singleton(this.databases[0]));

		assertEquals(1, balancer.size());

		balancer = this.factory.createBalancer(new HashSet<MockDatabase>(Arrays.asList(this.databases[0], this.databases[1])));
		
		assertEquals(2, balancer.size());

		balancer = this.factory.createBalancer(new HashSet<MockDatabase>(Arrays.asList(this.databases)));
		
		assertEquals(3, balancer.size());
	}

	/**
	 * Test method for {@link net.sf.hajdbc.balancer.AbstractBalancer#toArray()}.
	 */
	@Test
	public void toArray()
	{
		Balancer<Void, MockDatabase> balancer = this.factory.createBalancer(Collections.<MockDatabase>emptySet());

		assertTrue(Arrays.equals(new Object[0], balancer.toArray()));
		
		balancer = this.factory.createBalancer(Collections.singleton(this.databases[0]));

		assertTrue(Arrays.equals(new Object[] { this.databases[0] }, balancer.toArray()));

		balancer = this.factory.createBalancer(new HashSet<MockDatabase>(Arrays.asList(this.databases[0], this.databases[1])));
		
		assertTrue(Arrays.equals(new Object[] { this.databases[0], this.databases[1] }, balancer.toArray()));

		balancer = this.factory.createBalancer(new HashSet<MockDatabase>(Arrays.asList(this.databases)));
		
		assertTrue(Arrays.equals(this.databases, balancer.toArray()));
	}

	private static boolean assertCollectionEquals(Collection<MockDatabase> c1, Collection<MockDatabase> c2)
	{
		Iterator<MockDatabase> i1 = c1.iterator();
		Iterator<MockDatabase> i2 = c2.iterator();
		
		while (i1.hasNext() && i2.hasNext())
		{
			if (!i1.next().equals(i2.next())) return false;
		}
		
		return !i1.hasNext() && !i2.hasNext();
	}
	
	void complete(List<Future<Void>> futures) throws InterruptedException
	{
		for (Future<Void> future: futures)
		{
			try
			{
				future.get();
			}
			catch (ExecutionException e)
			{
				fail(e.getCause().toString());
			}
		}
	}
	
	class WaitingInvoker implements Invoker<Void, MockDatabase, Void, Void, Exception>
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
	
	static class InvocationTask implements Callable<Void>
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