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

import org.easymock.EasyMock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author  Paul Ferraro
 */
@SuppressWarnings("nls")
@Test
public abstract class AbstractTestBalancer implements Balancer<Void>
{
	private Balancer<Void> balancer;
	
	protected AbstractTestBalancer(Balancer<Void> balancer)
	{
		this.balancer = balancer;
	}
	
	@DataProvider(name = "database")
	Object[][] databaseProvider()
	{
		return new Object[][] { new Object[] { new MockDatabase("1", 1) } };
	}
	
	public void testAdd()
	{
		Database<Void> database = new MockDatabase("1", 1);
		
		boolean result = this.add(database);
		
		assert result;
		
		result = this.add(database);
		
		assert !result;
	}
	
	@Override
	public boolean add(Database<Void> database)
	{
		return this.balancer.add(database);
	}

	@SuppressWarnings("unchecked")
	public void testAfterInvocation()
	{
		Database<Void> database = EasyMock.createStrictMock(Database.class);
		
		EasyMock.replay(database);
		
		this.afterInvocation(database);
		
		EasyMock.verify(database);
	}
	
	@Override
	@Test(enabled = false)
	public void afterInvocation(Database<Void> database)
	{
		this.balancer.afterInvocation(database);
	}

	@SuppressWarnings("unchecked")
	public void testBeforeInvocation()
	{
		Database<Void> database = EasyMock.createStrictMock(Database.class);
		
		EasyMock.replay(database);
		
		this.beforeInvocation(database);
		
		EasyMock.verify(database);
	}

	@Override
	@Test(enabled = false)
	public void beforeInvocation(Database<Void> database)
	{
		this.balancer.beforeInvocation(database);
	}

	public void testAll()
	{
		Set<Database<Void>> databaseList = this.all();
		
		assert databaseList.isEmpty() : databaseList.size();
		
		Database<Void> database1 = new MockDatabase("db1", 1);
		this.add(database1);
		
		databaseList = this.all();
		
		assert databaseList.size() == 1 : databaseList.size();
		assert databaseList.contains(database1);
		
		Database<Void> database2 = new MockDatabase("db2", 1);
		this.add(database2);

		databaseList = this.all();

		assert databaseList.size() == 2 : databaseList.size();
		assert databaseList.contains(database1) && databaseList.contains(database2);

		this.remove(database1);

		databaseList = this.all();
		
		assert databaseList.size() == 1 : databaseList.size();
		assert databaseList.contains(database2);
		
		this.remove(database2);
		
		databaseList = this.all();
		
		assert databaseList.isEmpty() : databaseList.size();
	}
	
	@Override
	public Set<Database<Void>> all()
	{
		return this.balancer.all();
	}

	public void testEmptyNext()
	{
		try
		{
			Database<Void> database = this.next();
			
			assert false : database.getId();
		}
		catch (NoSuchElementException e)
		{
			assert true;
		}
	}

	public abstract void testNext();
	
	@Override
	public Database<Void> next()
	{
		return this.balancer.next();
	}

	public void testRemove()
	{
		Database<Void> database1 = new MockDatabase("1", 1);
		
		boolean removed = this.remove(database1);

		assert !removed;
		
		this.add(database1);

		Database<Void> database2 = new MockDatabase("2", 1);
		
		this.add(database2);
		
		removed = this.remove(database2);

		assert removed;
		
		removed = this.remove(database2);

		assert !removed;
		
		removed = this.remove(database1);

		assert removed;
		
		removed = this.remove(database1);

		assert !removed;
	}
	
	@Override
	public boolean remove(Database<Void> database)
	{
		return this.balancer.remove(database);
	}

	public void testClear()
	{
		int size = this.all().size();
		
		assert size == 0 : size;
		
		this.clear();
		
		assert size == 0 : size;
		
		this.add(new MockDatabase("1"));
		this.add(new MockDatabase("2"));
		
		size = this.all().size();
		
		assert size == 2 : size;
		
		this.clear();
				
		size = this.all().size();
		
		assert size == 0 : size;
	}
	
	@Override
	@AfterMethod
	public void clear()
	{
		this.balancer.clear();
	}
}
