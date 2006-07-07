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
package net.sf.hajdbc.balancer;

import java.sql.SQLException;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.MockDatabase;
import net.sf.hajdbc.Operation;

import org.testng.annotations.Test;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
@Test
public class TestLoadBalancer extends AbstractTestBalancer
{
	/**
	 * @see net.sf.hajdbc.balancer.AbstractTestBalancer#createBalancer()
	 */
	@Override
	protected Balancer createBalancer()
	{
		return new LoadBalancer();
	}

	/**
	 * @see net.sf.hajdbc.balancer.AbstractTestBalancer#next(net.sf.hajdbc.Balancer)
	 */
	@Override
	protected void next(Balancer balancer)
	{
		Database database0 = new MockDatabase("0", 0);
		Database database1 = new MockDatabase("1", 1);
		Database database2 = new MockDatabase("2", 2);

		balancer.add(database0);
		
		Database next = balancer.next();
		
		assert database0.equals(next) : next;

		balancer.add(database2);

		next = balancer.next();

		assert database2.equals(next) : next;
		
		balancer.add(database1);

		next = balancer.next();

		assert database2.equals(next) : next;
		
		// Add enough load to database2 to shift relative effective load
		Thread[] database2Threads = new Thread[2];
		for (int i = 0; i < 2; ++i)
		{
			database2Threads[i] = new OperationThread(balancer, new MockOperation(), database2);
			database2Threads[i].start();
		}
		
		next = balancer.next();
		
		assert database1.equals(next) : next;
		
		// Add enough load to database1 to shift relative effective load
		Thread database1Thread = new OperationThread(balancer, new MockOperation(), database1);
		database1Thread.start();

		next = balancer.next();
		
		assert database2.equals(next) : next;

		database1Thread.interrupt();

		next = balancer.next();

		assert database1.equals(next) : next;
		
		for (int i = 0; i < 2; ++i)
		{
			database2Threads[i].interrupt();
		}
		
		next = balancer.next();
		
		assert database2.equals(next) : next;
	}
	
	private static class OperationThread extends Thread
	{
		private Balancer balancer;
		private Operation<Void, Void> operation;
		private Database database;
		
		/**
		 * Constructs a new OperationThread.
		 * @param balancer
		 * @param operation 
		 * @param database
		 */
		public OperationThread(Balancer balancer, Operation<Void, Void> operation, Database database)
		{
			super();
			
			this.balancer = balancer;
			this.operation = operation;
			this.database = database;
			
			this.setDaemon(true);
		}
		
		/**
		 * @see java.lang.Runnable#run()
		 */
		@Override
		public void run()
		{
			this.balancer.beforeOperation(this.database);
			
			try
			{
				this.operation.execute(this.database, null);
				
				this.balancer.afterOperation(this.database);
			}
			catch (SQLException e)
			{
				assert false : e;
			}
		}

		/**
		 * @see java.lang.Thread#start()
		 */
		@Override
		public synchronized void start()
		{
			super.start();
			
			this.pause();
		}
		
		/**
		 * @see java.lang.Thread#interrupt()
		 */
		@Override
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
				assert false : e;
			}
		}
	}
	
	static class MockOperation implements Operation<Void, Void>
	{
		/**
		 * @see net.sf.hajdbc.Operation#execute(net.sf.hajdbc.Database, java.lang.Object)
		 */
		public Void execute(Database database, Void object)
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
