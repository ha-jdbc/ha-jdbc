/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2007 Paul Ferraro
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

import java.util.NoSuchElementException;
import java.util.Set;

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
@SuppressWarnings("nls")
public abstract class AbstractTestBalancer implements Balancer<Void>
{
	private Balancer<Void> balancer = createBalancer();
	
	protected abstract Balancer<Void> createBalancer();

	@AfterMethod
	void tearDown()
	{
		for (Database<Void> database: this.balancer.all())
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
	public boolean add(Database<Void> database)
	{
		boolean added = this.balancer.add(database);
		
		assert added;

		added = this.balancer.add(database);

		assert !added;
		
		return added;
	}

	/**
	 * @see net.sf.hajdbc.Balancer#afterInvocation(net.sf.hajdbc.Database)
	 */
	@Test(dataProvider = "database")
	public void afterInvocation(Database<Void> database)
	{
		this.balancer.add(database);
		
		this.balancer.beforeInvocation(database);
	}

	/**
	 * @see net.sf.hajdbc.Balancer#beforeInvocation(net.sf.hajdbc.Database)
	 */
	@Test(dataProvider = "database")
	public void beforeInvocation(Database<Void> database)
	{
		this.balancer.add(database);
		
		this.balancer.beforeInvocation(database);
	}

	/**
	 * @see net.sf.hajdbc.Balancer#list()
	 */
	@Test
	public Set<Database<Void>> all()
	{
		Set<Database<Void>> databaseList = this.balancer.all();
		
		assert databaseList.isEmpty() : databaseList.size();
		
		Database<Void> database1 = new MockDatabase("db1", 1);
		this.balancer.add(database1);
		
		databaseList = this.balancer.all();
		
		assert databaseList.size() == 1 : databaseList.size();
		assert databaseList.contains(database1);
		
		Database<Void> database2 = new MockDatabase("db2", 1);
		this.balancer.add(database2);

		databaseList = this.balancer.all();

		assert databaseList.size() == 2 : databaseList.size();
		assert databaseList.contains(database1) && databaseList.contains(database2);

		this.balancer.remove(database1);

		databaseList = this.balancer.all();
		
		assert databaseList.size() == 1 : databaseList.size();
		assert databaseList.contains(database2);
		
		this.balancer.remove(database2);
		
		databaseList = this.balancer.all();
		
		assert databaseList.isEmpty() : databaseList.size();
		
		return databaseList;
	}

	/**
	 * @see net.sf.hajdbc.Balancer#next()
	 */
	@Test
	public Database<Void> next()
	{
		try
		{
			Database<Void> database = this.balancer.next();
			
			assert false : database.getId();
		}
		catch (NoSuchElementException e)
		{
			assert true;
		}
		
		this.next(this.balancer);
		
		return null;
	}
	
	protected abstract void next(Balancer<Void> balancer);

	/**
	 * @see net.sf.hajdbc.Balancer#remove(net.sf.hajdbc.Database)
	 */
	@Test(dataProvider = "database")
	public boolean remove(Database<Void> database)
	{
		boolean removed = this.balancer.remove(database);

		assert !removed;
		
		this.balancer.add(database);

		Database<Void> database2 = new MockDatabase("2", 1);
		
		this.balancer.add(database2);
		
		removed = this.balancer.remove(database2);

		assert removed;
		
		removed = this.balancer.remove(database2);

		assert !removed;
		
		removed = this.balancer.remove(database);

		assert removed;
		
		removed = this.balancer.remove(database);

		assert !removed;
		
		return removed;
	}

	/**
	 * @see net.sf.hajdbc.Balancer#clear()
	 */
	@Override
	public void clear()
	{
		int size = this.balancer.all().size();
		
		assert size == 0 : size;
		
		this.balancer.clear();
		
		assert size == 0 : size;
		
		this.balancer.add(new MockDatabase("1"));
		this.balancer.add(new MockDatabase("2"));
		
		size = this.balancer.all().size();
		
		assert size == 2 : size;
		
		this.balancer.clear();
				
		size = this.balancer.all().size();
		
		assert size == 0 : size;
	}
}
