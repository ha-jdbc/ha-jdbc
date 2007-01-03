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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.hajdbc.Database;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public class LoadBalancer<D> extends AbstractBalancer<D>
{
	private Map<Database<D>, Integer> databaseMap = new HashMap<Database<D>, Integer>();
	
	/**
	 * @see net.sf.hajdbc.balancer.AbstractBalancer#collect()
	 */
	@Override
	protected Collection<Database<D>> collect()
	{
		return this.databaseMap.keySet();
	}

	/**
	 * @see net.sf.hajdbc.Balancer#add(net.sf.hajdbc.Database)
	 */
	@Override
	public synchronized boolean add(Database<D> database)
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
	public synchronized Database<D> next()
	{
		if (this.databaseMap.size() <= 1)
		{
			return this.first();
		}
		
		List<Map.Entry<Database<D>, Integer>> databaseMapEntryList = new ArrayList<Map.Entry<Database<D>, Integer>>(this.databaseMap.entrySet());
		
		Collections.sort(databaseMapEntryList, this.comparator);
		
		Map.Entry<Database<D>, Integer> mapEntry = databaseMapEntryList.get(0);
		
		return mapEntry.getKey();
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#beforeOperation(net.sf.hajdbc.Database)
	 */
	@Override
	public void beforeOperation(Database<D> database)
	{
		this.incrementLoad(database, 1);
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#afterOperation(net.sf.hajdbc.Database)
	 */
	@Override
	public void afterOperation(Database<D> database)
	{
		this.incrementLoad(database, -1);
	}
	
	private synchronized void incrementLoad(Database<D> database, int increment)
	{
		Integer load = this.databaseMap.remove(database);

		if (load != null)
		{
			this.databaseMap.put(database, load + increment);
		}
	}
	
	private Comparator<Map.Entry<Database<D>, Integer>> comparator = new Comparator<Map.Entry<Database<D>, Integer>>()
	{
		public int compare(Map.Entry<Database<D>, Integer> mapEntry1, Map.Entry<Database<D>, Integer> mapEntry2)
		{
			Database<D> database1 = mapEntry1.getKey();
			Database<D> database2 = mapEntry2.getKey();

			Integer load1 = mapEntry1.getValue();
			Integer load2 = mapEntry2.getValue();
			
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
	};
}
