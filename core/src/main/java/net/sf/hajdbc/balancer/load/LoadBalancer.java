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
package net.sf.hajdbc.balancer.load;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.balancer.AbstractBalancer;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.util.Collections;

/**
 * Balancer implementation whose {@link #next()} implementation returns the database with the least load.
 *
 * @author  Paul Ferraro
 * @param <D> either java.sql.Driver or javax.sql.DataSource
 */
public class LoadBalancer<Z, D extends Database<Z>> extends AbstractBalancer<Z, D>
{
	private final Lock lock = new ReentrantLock();
	
	private volatile SortedMap<D, AtomicInteger> databaseMap = Collections.emptySortedMap();
	
	private Comparator<Map.Entry<D, AtomicInteger>> comparator = (mapEntry1, mapEntry2) -> {
        D database1 = mapEntry1.getKey();
        D database2 = mapEntry2.getKey();

        float load1 = mapEntry1.getValue().get();
        float load2 = mapEntry2.getValue().get();
        
        int weight1 = database1.getWeight();
        int weight2 = database2.getWeight();
        
        // If weights are the same, we can simply compare the loads
        if (weight1 == weight2)
        {
            return Float.compare(load1, load2);
        }
        
        float weightedLoad1 = (weight1 != 0) ? (load1 / weight1) : Float.POSITIVE_INFINITY;
        float weightedLoad2 = (weight2 != 0) ? (load2 / weight2) : Float.POSITIVE_INFINITY;
        
        return Float.compare(weightedLoad1, weightedLoad2);
    };

	/**
	 * Constructs a new LoadBalancer
	 * @param databases
	 */
	public LoadBalancer(Set<D> databases)
	{
		if (databases.isEmpty())
		{
			this.databaseMap = Collections.emptySortedMap();
		}
		else if (databases.size() == 1)
		{
			this.databaseMap = Collections.singletonSortedMap(databases.iterator().next(), new AtomicInteger(1));
		}
		else
		{
			SortedMap<D, AtomicInteger> map = new TreeMap<>();
			
			for (D database: databases)
			{
				map.put(database, new AtomicInteger(1));
			}
			
			this.databaseMap = map;
		}
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
			return this.databaseMap.firstKey();
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
		return this.databaseMap.keySet();
	}

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
			SortedMap<D, AtomicInteger> addMap = new TreeMap<>(this.databaseMap);
			
			boolean added = false;
			
			for (D database: databases)
			{
				added = (addMap.put(database, new AtomicInteger(1)) == null) || added;
			}
			
			if (added)
			{
				this.databaseMap = addMap;
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
			SortedMap<D, AtomicInteger> map = new TreeMap<>(this.databaseMap);
			
			boolean removed = map.keySet().removeAll(databases);

			if (removed)
			{
				this.databaseMap = map;
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
			SortedMap<D, AtomicInteger> map = new TreeMap<>(this.databaseMap);
			
			boolean retained = map.keySet().retainAll(databases);

			if (retained)
			{
				this.databaseMap = map;
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
			if (!this.databaseMap.isEmpty())
			{
				this.databaseMap = new TreeMap<>();
			}
		}
		finally
		{
			this.lock.unlock();
		}
	}

	/**
	 * {@inheritDoc}
	 * @see java.util.Set#remove(java.lang.Object)
	 */
	@Override
	public boolean remove(Object database)
	{
		this.lock.lock();
		
		try
		{
			boolean remove = this.databaseMap.containsKey(database);
			
			if (remove)
			{
				if (this.databaseMap.size() == 1)
				{
					this.databaseMap = Collections.emptySortedMap();
				}
				else
				{
					SortedMap<D, AtomicInteger> map = new TreeMap<>(this.databaseMap);

					map.remove(database);
					
					this.databaseMap = map;
				}
			}
			
			return remove;
		}
		finally
		{
			this.lock.unlock();
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.balancer.Balancer#next()
	 */
	@Override
	public D next()
	{
		Set<Map.Entry<D, AtomicInteger>> entrySet = this.databaseMap.entrySet();
		
		return !entrySet.isEmpty() ? java.util.Collections.min(entrySet, this.comparator).getKey() : null;
	}

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
			boolean add = !this.databaseMap.containsKey(database);
			
			if (add)
			{
				AtomicInteger load = new AtomicInteger(1);
				
				if (this.databaseMap.isEmpty())
				{
					this.databaseMap = Collections.singletonSortedMap(database, load);
				}
				else
				{
					SortedMap<D, AtomicInteger> map = new TreeMap<>(this.databaseMap);
					
					map.put(database, load);
					
					this.databaseMap = map;
				}
			}
			
			return add;
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.balancer.Balancer#invoke(net.sf.hajdbc.invocation.Invoker, net.sf.hajdbc.Database, java.lang.Object)
	 */
	@Override
	public <T, R, E extends Exception> R invoke(Invoker<Z, D, T, R, E> invoker, D database, T object) throws E
	{
		AtomicInteger load = this.databaseMap.get(database);
		
		if (load != null)
		{
			load.incrementAndGet();
		}
		
		try
		{
			return invoker.invoke(database, object);
		}
		finally
		{
			if (load != null)
			{
				load.decrementAndGet();
			}
		}
	}
}
