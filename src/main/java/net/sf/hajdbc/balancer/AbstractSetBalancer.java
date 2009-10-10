/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2009 Paul Ferraro
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

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import net.sf.hajdbc.Database;

/**
 * @author paul
 *
 */
public abstract class AbstractSetBalancer<Z, D extends Database<Z>> extends AbstractBalancer<Z, D>
{
	private final Lock lock = new ReentrantLock();

	private volatile Set<D> databaseSet = Collections.emptySet();

	protected Lock getLock()
	{
		return this.lock;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.balancer.AbstractBalancer#getDatabaseSet()
	 */
	@Override
	protected Set<D> getDatabaseSet()
	{
		return this.databaseSet;
	}

	/**
	 * @see net.sf.hajdbc.Balancer#beforeInvocation(net.sf.hajdbc.Database)
	 */
	@Override
	public void beforeInvocation(D database)
	{
		// Do nothing
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#afterInvocation(net.sf.hajdbc.Database)
	 */
	@Override
	public void afterInvocation(D database)
	{
		// Do nothing
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#remove(net.sf.hajdbc.Database)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public boolean remove(Object database)
	{
		this.lock.lock();
		
		try
		{
			boolean remove = this.databaseSet.contains(database);

			if (remove)
			{
				SortedSet<D> set = new TreeSet<D>(this.databaseSet);
				
				set.remove(database);
				
				this.databaseSet = set;
				
				this.removed((D) database);
			}
			
			return remove;
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
	protected abstract void removed(D database);
	
	/**
	 * @see net.sf.hajdbc.Balancer#add(net.sf.hajdbc.Database)
	 */
	@Override
	public boolean add(D database)
	{
		this.lock.lock();
		
		try
		{
			boolean add = !this.databaseSet.contains(database);
			
			if (add)
			{
				SortedSet<D> set = new TreeSet<D>(this.databaseSet);
				
				set.add(database);
				
				this.databaseSet = set;
				
				this.added(database);
			}
			
			return add;
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
	protected abstract void added(D database);

	/**
	 * {@inheritDoc}
	 * @see java.util.Set#addAll(java.util.Collection)
	 */
	@Override
	public boolean addAll(Collection<? extends D> databases)
	{
		this.lock.lock();
		
		try
		{
			SortedSet<D> addSet = new TreeSet<D>(this.databaseSet);

			boolean added = addSet.addAll(databases);
			
			if (added)
			{
				Set<D> removeSet = new TreeSet<D>(addSet);
				
				removeSet.removeAll(this.databaseSet);
				
				this.databaseSet = addSet;
				
				for (D database: removeSet)
				{
					this.added(database);
				}
			}
			
			return added;
		}
		finally
		{
			this.lock.unlock();
		}
	}

	/**
	 * {@inheritDoc}
	 * @see java.util.Set#removeAll(java.util.Collection)
	 */
	@Override
	public boolean removeAll(Collection<?> databases)
	{
		this.lock.lock();
		
		try
		{
			SortedSet<D> removeSet = new TreeSet<D>(this.databaseSet);

			boolean removed = removeSet.removeAll(databases);
			
			if (removed)
			{
				Set<D> retainSet = new TreeSet<D>(this.databaseSet);
				
				retainSet.retainAll(databases);
				
				this.databaseSet = removeSet;
				
				for (D database: removeSet)
				{
					this.removed(database);
				}
			}
			
			return removed;
		}
		finally
		{
			this.lock.unlock();
		}
	}

	/**
	 * {@inheritDoc}
	 * @see java.util.Set#retainAll(java.util.Collection)
	 */
	@Override
	public boolean retainAll(Collection<?> databases)
	{
		this.lock.lock();
		
		try
		{
			SortedSet<D> retainSet = new TreeSet<D>(this.databaseSet);

			boolean retained = retainSet.retainAll(databases);
			
			if (retained)
			{
				Set<D> removeSet = new TreeSet<D>(this.databaseSet);
				
				removeSet.removeAll(databases);
				
				this.databaseSet = retainSet;
				
				for (D database: removeSet)
				{
					this.removed(database);
				}
			}
			
			return retained;
		}
		finally
		{
			this.lock.unlock();
		}
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
			if (!this.databaseSet.isEmpty())
			{
				this.databaseSet = Collections.emptySet();
				
				this.cleared();
			}
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
