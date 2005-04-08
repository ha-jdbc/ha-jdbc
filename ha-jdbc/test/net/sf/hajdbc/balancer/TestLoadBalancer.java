/*
 * Copyright (c) 2004, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc.balancer;

import java.sql.SQLException;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.Operation;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class TestLoadBalancer extends TestBalancer
{
	/**
	 * @see net.sf.hajdbc.balancer.TestBalancer#createBalancer()
	 */
	protected Balancer createBalancer()
	{
		return new LoadBalancer();
	}

	/**
	 * @see net.sf.hajdbc.balancer.TestBalancer#testNext(net.sf.hajdbc.Balancer)
	 */
	protected void testNext(Balancer balancer)
	{
		Database database0 = new MockDatabase("0", 0);
		Database database1 = new MockDatabase("1", 1);
		Database database2 = new MockDatabase("2", 2);

		balancer.add(database0);
		
		Database next = balancer.next();
		
		assert next.equals(database0) : "#1: next() = " + next + ", expected " + database0;

		balancer.add(database2);

		next = balancer.next();
		
		assert next.equals(database2) : "#2: next() = " + next + ", expected " + database2;
		
		balancer.add(database1);

		next = balancer.next();
		
		assert next.equals(database2) : "#3: next() = " + next + ", expected " + database2;
		
		// Add enough load to database2 to shift relative effective load
		Thread[] database2Threads = new Thread[2];
		for (int i = 0; i < 2; ++i)
		{
			database2Threads[i] = new OperationThread(balancer, database2);
			database2Threads[i].start();
		}
		
		next = balancer.next();
		
		assert next.equals(database1) : "#4: next() = " + next + ", expected " + database1;
		
		// Add enough load to database1 to shift relative effective load
		Thread database1Thread = new OperationThread(balancer, database1);
		database1Thread.start();

		next = balancer.next();
		
		assert next.equals(database2) : "#5: next() = " + next + ", expected " + database2;

		database1Thread.interrupt();

		next = balancer.next();
		
		assert next.equals(database1) : "#6: next() = " + next + ", expected " + database1;
		
		for (int i = 0; i < 2; ++i)
		{
			database2Threads[i].interrupt();
		}
		
		next = balancer.next();
		
		assert next.equals(database2) : "#7: next() = " + next + ", expected " + database2;
	}
	
	private class OperationThread extends Thread
	{
		private Balancer balancer;
		private Database database;
		
		/**
		 * Constructs a new OperationThread.
		 * @param balancer
		 * @param database
		 */
		public OperationThread(Balancer balancer, Database database)
		{
			super();
			
			this.balancer = balancer;
			this.database = database;
			
			this.setDaemon(true);
		}
		
		/**
		 * @see java.lang.Runnable#run()
		 */
		public void run()
		{
			try
			{
				this.balancer.execute(new MockOperation(), this.database, null);
			}
			catch (SQLException e)
			{
				assert false;
			}
		}

		/**
		 * @see java.lang.Thread#start()
		 */
		public synchronized void start()
		{
			super.start();
			
			this.pause();
		}
		
		/**
		 * @see java.lang.Thread#interrupt()
		 */
		public void interrupt()
		{
			super.interrupt();
			
			this.pause();
		}

		private void pause()
		{
			try
			{
				Thread.sleep(10);
			}
			catch (InterruptedException e)
			{
				assert false;
			}
		}
	}
	
	private class MockOperation implements Operation
	{
		/**
		 * @see net.sf.hajdbc.Operation#execute(net.sf.hajdbc.Database, java.lang.Object)
		 */
		public Object execute(Database database, Object sqlObject)
		{
			// Simulate a long operation
			try
			{
				Thread.sleep(10000);
			}
			catch (InterruptedException e)
			{
				// Ignore
			}
			
			return null;
		}
	}
}
