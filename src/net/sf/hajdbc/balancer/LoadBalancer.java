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

import java.util.Collection;
import java.util.Comparator;
import java.util.SortedMap;
import java.util.TreeMap;

import net.sf.hajdbc.Database;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public class LoadBalancer extends AbstractBalancer
{
	protected SortedMap<Database, Integer> databaseMap = new TreeMap<Database, Integer>(new LoadComparator());
	
	/**
	 * @see net.sf.hajdbc.Balancer#all()
	 */
	@Override
	protected Collection<Database> getDatabases()
	{
		return this.databaseMap.keySet();
	}

	/**
	 * @see net.sf.hajdbc.Balancer#add(net.sf.hajdbc.Database)
	 */
	@Override
	public synchronized boolean add(Database database)
	{
		boolean exists = this.databaseMap.containsKey(database);
		
		if (!exists)
		{
			this.databaseMap.put(database, 1);
		}
		
		return !exists;
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#next()
	 */
	public synchronized Database next()
	{
		return this.first();
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#first()
	 */
	@Override
	public synchronized Database first()
	{
		return this.databaseMap.firstKey();
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#beforeOperation(net.sf.hajdbc.Database)
	 */
	@Override
	public void beforeOperation(Database database)
	{
		this.incrementLoad(database, 1);
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#afterOperation(net.sf.hajdbc.Database)
	 */
	@Override
	public void afterOperation(Database database)
	{
		this.incrementLoad(database, -1);
	}
	
	private synchronized void incrementLoad(Database database, int increment)
	{
		Integer load = this.databaseMap.remove(database);

		if (load != null)
		{
			this.databaseMap.put(database, load + increment);
		}
	}
	
	private class LoadComparator implements Comparator<Database>
	{
		/**
		 * @see java.util.Comparator#compare(T, T)
		 */
		public int compare(Database database1, Database database2)
		{
			Integer load1 = LoadBalancer.this.databaseMap.get(database1);
			Integer load2 = LoadBalancer.this.databaseMap.get(database1);
			
			int weight1 = database1.getWeight();
			int weight2 = database2.getWeight();
			
			if (weight1 == weight2)
			{
				return load1.compareTo(load2);
			}
			
			float weightedLoad1 = (weight1 != 0) ? (load1.floatValue() / weight1) : Float.POSITIVE_INFINITY;
			float weightedLoad2 = (weight2 != 0) ? (load2.floatValue() / weight2) : Float.POSITIVE_INFINITY;
			
			return Float.compare(weightedLoad1, weightedLoad2);
		}
	}
}
