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
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.Database;

/**
 * Balancer implementation whose {@link #next()} implementation returns the database with the least load.
 *
 * @author  Paul Ferraro
 * @param <D> either java.sql.Driver or javax.sql.DataSource
 */
public class LoadBalancer<D> implements Balancer<D>
{
	private volatile Map<Database<D>, AtomicInteger> databaseMap = Collections.emptyMap();

	private Lock lock = new ReentrantLock();
	
	private Comparator<Map.Entry<Database<D>, AtomicInteger>> comparator = new Comparator<Map.Entry<Database<D>, AtomicInteger>>()
	{
		public int compare(Map.Entry<Database<D>, AtomicInteger> mapEntry1, Map.Entry<Database<D>, AtomicInteger> mapEntry2)
		{
			Database<D> database1 = mapEntry1.getKey();
			Database<D> database2 = mapEntry2.getKey();

			float load1 = mapEntry1.getValue().get();
			float load2 = mapEntry1.getValue().get();
			
			int weight1 = database1.getWeight();
			int weight2 = database2.getWeight();
			
			if (weight1 == weight2)
			{
				Float.compare(load1, load2);
			}
			
			float weightedLoad1 = (weight1 != 0) ? (load1 / weight1) : Float.POSITIVE_INFINITY;
			float weightedLoad2 = (weight2 != 0) ? (load2 / weight2) : Float.POSITIVE_INFINITY;
			
			return Float.compare(weightedLoad1, weightedLoad2);
		}
	};
	
	/**
	 * @see net.sf.hajdbc.Balancer#all()
	 */
	@Override
	public Set<Database<D>> all()
	{
		return Collections.unmodifiableSet(this.databaseMap.keySet());
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
			this.databaseMap = Collections.emptyMap();
		}
		finally
		{
			this.lock.unlock();
		}
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
			boolean exists = this.databaseMap.containsKey(database);
			
			if (exists)
			{
				Map<Database<D>, AtomicInteger> map = new TreeMap<Database<D>, AtomicInteger>(this.databaseMap);
				
				map.remove(database);

				this.databaseMap = map;
			}
			
			return exists;
		}
		finally
		{
			this.lock.unlock();
		}
	}

	/**
	 * @see net.sf.hajdbc.Balancer#next()
	 */
	@Override
	public Database<D> next()
	{
		return Collections.min(this.databaseMap.entrySet(), this.comparator).getKey();
	}

	/**
	 * @see net.sf.hajdbc.Balancer#add(net.sf.hajdbc.Database)
	 */
	@Override
	public boolean add(Database<D> database)
	{
		this.lock.lock();
		
		try
		{
			boolean exists = this.databaseMap.containsKey(database);
			
			if (!exists)
			{
				Map<Database<D>, AtomicInteger> map = new TreeMap<Database<D>, AtomicInteger>(this.databaseMap);
				
				map.put(database, new AtomicInteger(1));

				this.databaseMap = map;
			}
			
			return !exists;
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#beforeInvocation(net.sf.hajdbc.Database)
	 */
	@Override
	public void beforeInvocation(Database<D> database)
	{
		AtomicInteger load = this.databaseMap.get(database);
		
		if (load != null)
		{
			load.incrementAndGet();
		}
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#afterInvocation(net.sf.hajdbc.Database)
	 */
	@Override
	public void afterInvocation(Database<D> database)
	{
		AtomicInteger load = this.databaseMap.get(database);
		
		if (load != null)
		{
			load.decrementAndGet();
		}
	}
}
