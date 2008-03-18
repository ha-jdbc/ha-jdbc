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

import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.Database;

/**
 * Thread-safe abstract balancer implementation that implements most of the Balancer interface, except {@link Balancer#next()}.
 * Uses A copy-on-write algorithm for {@link #add(Database)}, {@link #remove(Database)}, and {@link #clear()}.
 * Calls to {@link #all()} are non-blocking.
 * 
 * @author  Paul Ferraro
 * @param <D> either java.sql.Driver or javax.sql.DataSource
 */
public abstract class AbstractBalancer<D> implements Balancer<D>
{
	protected Lock lock = new ReentrantLock();

	protected volatile SortedSet<Database<D>> databaseSet = new TreeSet<Database<D>>();

	/**
	 * @see net.sf.hajdbc.Balancer#beforeInvocation(net.sf.hajdbc.Database)
	 */
	@Override
	public void beforeInvocation(Database<D> database)
	{
		// Do nothing
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#afterInvocation(net.sf.hajdbc.Database)
	 */
	@Override
	public void afterInvocation(Database<D> database)
	{
		// Do nothing
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#remove(net.sf.hajdbc.Database)
	 */
	@Override
	public boolean remove(Database<D> database)
	{
		this.lock.lock();
		
		try
		{
			boolean exists = this.databaseSet.contains(database);
			
			if (exists)
			{
				SortedSet<Database<D>> set = new TreeSet<Database<D>>(this.databaseSet);
				
				set.remove(database);
				
				this.databaseSet = set;
				
				this.removed(database);
			}
			
			return exists;
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	/**
	 * Called when a database was removed from the set.
	 * @param database a database descriptor
	 */
	protected abstract void removed(Database<D> database);
	
	/**
	 * @see net.sf.hajdbc.Balancer#add(net.sf.hajdbc.Database)
	 */
	@Override
	public boolean add(Database<D> database)
	{
		this.lock.lock();
		
		try
		{
			boolean exists = this.databaseSet.contains(database);
			
			if (!exists)
			{
				SortedSet<Database<D>> set = new TreeSet<Database<D>>(this.databaseSet);
				
				set.add(database);
				
				this.databaseSet = set;
					
				this.added(database);
			}
			
			return !exists;
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	/**
	 * Called when a database was added to the set.
	 * @param database a database descriptor
	 */
	protected abstract void added(Database<D> database);
	
	/**
	 * @see net.sf.hajdbc.Balancer#all()
	 */
	@Override
	public Set<Database<D>> all()
	{
		return Collections.unmodifiableSet(this.databaseSet);
	}

	/**
	 * @see net.sf.hajdbc.Balancer#clear()
	 */
	@Override
	public void clear()
	{
		this.lock.lock();
		
		try
		{
			this.databaseSet = new TreeSet<Database<D>>();
			
			this.cleared();
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	/**
	 * Called when the set was cleared.
	 */
	protected abstract void cleared();
}
