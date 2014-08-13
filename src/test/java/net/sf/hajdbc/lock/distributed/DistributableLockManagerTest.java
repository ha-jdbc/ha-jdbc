/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2014  Paul Ferraro
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
package net.sf.hajdbc.lock.distributed;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.distributed.CommandDispatcherFactory;
import net.sf.hajdbc.distributed.jgroups.JGroupsCommandDispatcherFactory;
import net.sf.hajdbc.lock.LockManager;
import net.sf.hajdbc.lock.semaphore.SemaphoreLockManager;

/**
 * @author Paul Ferraro
 */
public class DistributableLockManagerTest
{
	LockManager manager1;
	LockManager manager2;

	@Before
	public void init() throws Exception
	{
		String id = "cluster";
		DatabaseCluster<?, ?> cluster1 = mock(DatabaseCluster.class);
		DatabaseCluster<?, ?> cluster2 = mock(DatabaseCluster.class);
		LockManager lockManager1 = new SemaphoreLockManager(false);
		LockManager lockManager2 = new SemaphoreLockManager(false);
		CommandDispatcherFactory dispatcherFactory1 = createCommandDispatcherFactory("node1");
		CommandDispatcherFactory dispatcherFactory2 = createCommandDispatcherFactory("node2");

		when(cluster1.getId()).thenReturn(id);
		when(cluster1.getLockManager()).thenReturn(lockManager1);
		when(cluster2.getId()).thenReturn(id);
		when(cluster2.getLockManager()).thenReturn(lockManager2);

		this.manager1 = new DistributedLockManager(cluster1, dispatcherFactory1);
		this.manager1.start();
		this.manager2 = new DistributedLockManager(cluster2, dispatcherFactory2);
		this.manager2.start();
	}

	static CommandDispatcherFactory createCommandDispatcherFactory(String name)
	{
		JGroupsCommandDispatcherFactory factory = new JGroupsCommandDispatcherFactory();
		factory.setName(name);
		factory.setStack("fast-local.xml");
		return factory;
	}

	@After
	public void destroy()
	{
		this.manager1.stop();
		this.manager1 = null;
		this.manager2.stop();
		this.manager2 = null;
	}

	@Test
	public void simple()
	{
		test(this.manager1);
		test(this.manager2);
	}

	private static void test(LockManager manager)
	{
		test(manager.readLock("1"));
		test(manager.readLock(null));
		test(manager.writeLock("1"));
		test(manager.writeLock(null));
	}

	private static void test(Lock lock)
	{
		assertTrue(lock.tryLock());
		lock.unlock();
	}

	@Test
	public void blocking()
	{
		blocking(this.manager1, this.manager2);
		blocking(this.manager2, this.manager1);
	}

	private static void blocking(LockManager manager1, LockManager manager2)
	{
		Lock readLock = manager1.readLock(null);
		Lock readLock2 = manager2.readLock(null);
		Lock writeLock = manager2.writeLock(null);
		
		assertTrue(readLock.tryLock());
		
		try
		{
			// Validate that reads do not block on another
			assertTrue(readLock2.tryLock());
			readLock2.unlock();
			
			// Validate that remote read blocks local write
			boolean locked = writeLock.tryLock();
			try
			{
				assertFalse(locked);
			}
			finally
			{
				if (locked)
				{
					writeLock.unlock();
				}
			}
		}
		finally
		{
			readLock.unlock();
		}
		
		assertTrue(writeLock.tryLock());
		
		try
		{
			// Validate that remote write blocks local read
			boolean locked = readLock.tryLock();
			try
			{
				assertFalse(locked);
			}
			finally
			{
				if (locked)
				{
					readLock.unlock();
				}
			}
		}
		finally
		{
			writeLock.unlock();
		}
	}
	
	@Test
	public void failover() throws Exception
	{
		Lock lock1 = this.manager1.writeLock(null);
		Lock lock2 = this.manager2.writeLock(null);

		assertTrue(lock1.tryLock());
		boolean locked = lock2.tryLock();
		try
		{
			assertFalse(locked);
		}
		finally
		{
			if (locked)
			{
				lock2.unlock();
			}
		}
		
		ExecutorService executor = Executors.newSingleThreadExecutor();
		try
		{
			Callable<Void> task = new Callable<Void>()
			{
				@Override
				public Void call() throws Exception
				{
					DistributableLockManagerTest.this.manager1.stop();
					DistributableLockManagerTest.this.manager1.start();
					return null;
				}
			};
			Future<?> future = executor.submit(task);
			
			locked = lock2.tryLock(10, TimeUnit.SECONDS);
			
			assertTrue(locked);
			lock2.unlock();
			
			future.get();
		}
		finally
		{
			executor.shutdownNow();
		}
	}
}
