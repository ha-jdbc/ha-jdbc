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

import java.util.List;
import java.util.NoSuchElementException;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.MockDatabase;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public abstract class AbstractTestBalancer implements Balancer
{
	private Balancer balancer = createBalancer();
	
	protected abstract Balancer createBalancer();

	@AfterMethod
	void tearDown()
	{
		for (Database database: this.balancer.list())
		{
			this.balancer.remove(database);
		}
	}
	
	@DataProvider(name = "database")
	Object[][] databaseProvider()
	{
		return new Object[][] { new Object[] { new MockDatabase("1", 1) } };
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#add(net.sf.hajdbc.Database)
	 */
	@Test(dataProvider = "database")
	public boolean add(Database database)
	{
		boolean added = this.balancer.add(database);
		
		assert added;

		added = this.balancer.add(database);

		assert !added;
		
		return added;
	}

	/**
	 * @see net.sf.hajdbc.Balancer#afterOperation(net.sf.hajdbc.Database)
	 */
	@Test(dataProvider = "database")
	public void afterOperation(Database database)
	{
		this.balancer.add(database);
		
		this.balancer.beforeOperation(database);
	}

	/**
	 * @see net.sf.hajdbc.Balancer#beforeOperation(net.sf.hajdbc.Database)
	 */
	@Test(dataProvider = "database")
	public void beforeOperation(Database database)
	{
		this.balancer.add(database);
		
		this.balancer.beforeOperation(database);
	}

	/**
	 * @see net.sf.hajdbc.Balancer#contains(net.sf.hajdbc.Database)
	 */
	@Test(dataProvider = "database")
	public boolean contains(Database database)
	{
		boolean contains = this.balancer.contains(database);
		
		assert !contains;
		
		this.balancer.add(database);
		
		contains = this.balancer.contains(database);
		
		assert contains;
		
		return contains;
	}

	/**
	 * @see net.sf.hajdbc.Balancer#first()
	 */
	@Test
	public Database first()
	{
		try
		{
			this.balancer.first();
			
			assert false;
		}
		catch (NoSuchElementException e)
		{
			assert true;
		}
		
		Database database = new MockDatabase("0", 0);
		
		this.balancer.add(database);
		
		Database first = this.balancer.first();

		assert database.equals(first) : database;
		
		return database;
	}

	/**
	 * @see net.sf.hajdbc.Balancer#list()
	 */
	@Test
	public List<Database> list()
	{
		List<Database> databaseList = this.balancer.list();
		
		assert databaseList.isEmpty() : databaseList.size();
		
		Database database1 = new MockDatabase("db1", 1);
		this.balancer.add(database1);
		
		databaseList = this.balancer.list();
		
		assert databaseList.size() == 1 : databaseList.size();
		assert databaseList.contains(database1);
		
		Database database2 = new MockDatabase("db2", 1);
		this.balancer.add(database2);

		databaseList = this.balancer.list();

		assert databaseList.size() == 2 : databaseList.size();
		assert databaseList.contains(database1) && databaseList.contains(database2);

		this.balancer.remove(database1);

		databaseList = this.balancer.list();
		
		assert databaseList.size() == 1 : databaseList.size();
		assert databaseList.contains(database2);
		
		this.balancer.remove(database2);
		
		databaseList = this.balancer.list();
		
		assert databaseList.isEmpty() : databaseList.size();
		
		return databaseList;
	}

	/**
	 * @see net.sf.hajdbc.Balancer#next()
	 */
	@Test
	public Database next()
	{
		try
		{
			Database database = this.balancer.next();
			
			assert false : database.getId();
		}
		catch (NoSuchElementException e)
		{
			assert true;
		}
		
		this.next(this.balancer);
		
		return null;
	}
	
	protected abstract void next(Balancer balancer);

	/**
	 * @see net.sf.hajdbc.Balancer#remove(net.sf.hajdbc.Database)
	 */
	@Test(dataProvider = "database")
	public boolean remove(Database database)
	{
		boolean removed = this.balancer.remove(database);

		assert !removed;
		
		this.balancer.add(database);

		removed = this.balancer.remove(database);

		assert removed;
		
		removed = this.balancer.remove(database);

		assert !removed;
		
		return removed;
	}
}
