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

import java.sql.Connection;
import java.util.Arrays;
import java.util.Collection;
import java.util.NoSuchElementException;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.Database;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public abstract class AbstractTestBalancer extends junit.framework.TestCase
{
	private Balancer balancer;
	
	/**
	 * @see junit.framework.TestCase#setUp()
	 */
	protected void setUp()
	{
		this.balancer = createBalancer();
	}
	
	protected abstract Balancer createBalancer();
	
	/**
	 * Test method for {@link Balancer#add(Database)}
	 */
	public void testAdd()
	{
		Database database = new MockDatabase("1", 1);
		
		boolean added = this.balancer.add(database);
		
		assertTrue(added);

		added = this.balancer.add(database);
		
		assertFalse(added);
	}

	/**
	 * Test method for {@link Balancer#beforeOperation(Database)}
	 */
	public void testBeforeOperation()
	{
		Database database = new MockDatabase("db1", 1);

		this.balancer.add(database);
		
		this.balancer.beforeOperation(database);
	}

	/**
	 * Test method for {@link Balancer#afterOperation(Database)}
	 */
	public void testAfterOperation()
	{
		Database database = new MockDatabase("db1", 1);

		this.balancer.add(database);
		
		this.balancer.afterOperation(database);
	}
	
	/**
	 * Test method for {@link Balancer#remove(Database)}
	 */
	public void testRemove()
	{
		Database database = new MockDatabase("1", 1);
		
		boolean removed = this.balancer.remove(database);

		assertFalse(removed);
		
		this.balancer.add(database);

		removed = this.balancer.remove(database);

		assertTrue(removed);
		
		removed = this.balancer.remove(database);

		assertFalse(removed);
	}

	/**
	 * Test method for {@link Balancer#list()}
	 */
	public void testGetDatabases()
	{
		Collection<Database> databases = this.balancer.list();
		
		assertEquals(databases.size(), 0);
		
		Database database1 = new MockDatabase("db1", 1);
		this.balancer.add(database1);
		
		databases = this.balancer.list();
		
		assertEquals(databases.size(), 1);
		assertTrue(Arrays.equals(databases.toArray(), new Database[] { database1 }));
		
		Database database2 = new MockDatabase("db2", 1);
		this.balancer.add(database2);

		databases = this.balancer.list();

		assertEquals(databases.size(), 2);
		assertTrue(Arrays.equals(databases.toArray(), new Database[] { database1, database2 }) || Arrays.equals(databases.toArray(), new Database[] { database2, database1 }));

		this.balancer.remove(database1);

		databases = this.balancer.list();
		
		assertEquals(databases.size(), 1);
		assertTrue(Arrays.equals(databases.toArray(), new Database[] { database2, }));
		
		this.balancer.remove(database2);
		
		databases = this.balancer.list();
		
		assertEquals(databases.size(), 0);
	}

	/**
	 * Test method for {@link Balancer#contains(Database)}
	 */
	public void testContains()
	{
		Database database1 = new MockDatabase("db1", 1);
		Database database2 = new MockDatabase("db2", 1);

		this.balancer.add(database1);
		
		assertTrue(this.balancer.contains(database1));
		assertFalse(this.balancer.contains(database2));
	}

	/**
	 * Test method for {@link Balancer#first()}
	 */
	public void testFirst()
	{
		try
		{
			this.balancer.first();
			
			fail();
		}
		catch (NoSuchElementException e)
		{
			// Do nothing
		}
		
		Database database = new MockDatabase("0", 0);
		
		this.balancer.add(database);
		
		Database first = this.balancer.first();
		
		assertEquals(database, first);
	}

	/**
	 * Test method for {@link Balancer#next()}
	 */
	public void testNext()
	{
		try
		{
			this.balancer.next();
			
			fail();
		}
		catch (NoSuchElementException e)
		{
			// Do nothing
		}
		
		testNext(this.balancer);
	}
	
	protected abstract void testNext(Balancer balancer);
	
	protected class MockDatabase implements Database
	{
		private String id;
		private Integer weight;
		
		protected MockDatabase(String id, int weight)
		{
			this.id = id;
			this.weight = weight;
		}
		
		/**
		 * @see net.sf.hajdbc.Database#getId()
		 */
		public String getId()
		{
			return this.id;
		}

		/**
		 * @see net.sf.hajdbc.Database#connect(java.lang.Object)
		 */
		public Connection connect(Object connectionFactory)
		{
			return null;
		}

		/**
		 * @see net.sf.hajdbc.Database#createConnectionFactory()
		 */
		public Object createConnectionFactory()
		{
			return null;
		}

		/**
		 * @see net.sf.hajdbc.Database#getWeight()
		 */
		public Integer getWeight()
		{
			return this.weight;
		}
		
		/**
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		public boolean equals(Object object)
		{
			Database database = (Database) object;
			
			return this.id.equals(database.getId());
		}
		
		/**
		 * @see java.lang.Object#toString()
		 */
		public String toString()
		{
			return this.id;
		}
	}
}
