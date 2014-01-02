/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
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

import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.util.Collections;

/**
 * Abstract set-based {@link Balancer} implementation.
 * @author Paul Ferraro
 */
public abstract class AbstractSetBalancer<Z, D extends Database<Z>> extends AbstractBalancer<Z, D>
{
	private final Lock lock = new ReentrantLock();

	private volatile SortedSet<D> databaseSet;

	protected AbstractSetBalancer(Set<D> databases)
	{
		if (databases.isEmpty())
		{
			this.databaseSet = Collections.emptySortedSet();
		}
		else if (databases.size() == 1)
		{
			this.databaseSet = Collections.singletonSortedSet(databases.iterator().next());
		}
		else
		{
			SortedSet<D> set = new TreeSet<>();
			
			for (D database: databases)
			{
				set.add(database);
			}
			
			this.databaseSet = set;
		}
	}

	protected Lock getLock()
	{
		return this.lock;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.balancer.Balancer#invoke(net.sf.hajdbc.invocation.Invoker, net.sf.hajdbc.Database, java.lang.Object)
	 */
	@Override
	public <T, R, E extends Exception> R invoke(Invoker<Z, D, T, R, E> invoker, D database, T object) throws E
	{
		return invoker.invoke(database, object);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.balancer.Balancer#primary()
	 */
	@Override
	public D primary()
	{
		try
		{
			return this.databaseSet.first();
		}
		catch (NoSuchElementException e)
		{
			return null;
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.balancer.AbstractBalancer#getDatabases()
	 */
	@Override
	protected Set<D> getDatabases()
	{
		return this.databaseSet;
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.util.Set#remove(java.lang.Object)
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
				if (this.databaseSet.size() == 1)
				{
					this.databaseSet = Collections.emptySortedSet();
				}
				else
				{
					SortedSet<D> set = new TreeSet<>(this.databaseSet);
					
					set.remove(database);
					
					this.databaseSet = set;
				}
				
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
	 * {@inheritDoc}
	 * @see java.util.Set#add(java.lang.Object)
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
				if (this.databaseSet.isEmpty())
				{
					this.databaseSet = Collections.singletonSortedSet(database);
				}
				else
				{
					SortedSet<D> set = new TreeSet<>(this.databaseSet);
					
					set.add(database);
					
					this.databaseSet = set;
				}
				
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
			SortedSet<D> addSet = new TreeSet<>(this.databaseSet);

			boolean added = addSet.addAll(databases);
			
			if (added)
			{
				Set<D> removeSet = new TreeSet<>(addSet);
				
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
			SortedSet<D> removeSet = new TreeSet<>(this.databaseSet);

			boolean removed = removeSet.removeAll(databases);
			
			if (removed)
			{
				Set<D> retainSet = new TreeSet<>(this.databaseSet);
				
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
			SortedSet<D> retainSet = new TreeSet<>(this.databaseSet);

			boolean retained = retainSet.retainAll(databases);
			
			if (retained)
			{
				Set<D> removeSet = new TreeSet<>(this.databaseSet);
				
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
	 * {@inheritDoc}
	 * @see java.util.Set#clear()
	 */
	@Override
	public void clear()
	{
		this.lock.lock();
		
		try
		{
			if (!this.databaseSet.isEmpty())
			{
				this.databaseSet = Collections.emptySortedSet();
				
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
