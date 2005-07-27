/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2004 Paul Ferraro
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
public class LoadBalancer extends AbstractBalancer
{
	private static Comparator comparator = new Comparator()
	{
		public int compare(Object object1, Object object2)
		{
			Map.Entry databaseMapEntry1 = (Map.Entry) object1;
			Map.Entry databaseMapEntry2 = (Map.Entry) object2;
			
			Database database1 = (Database) databaseMapEntry1.getKey();
			Database database2 = (Database) databaseMapEntry2.getKey();

			Integer load1 = (Integer) databaseMapEntry1.getValue();
			Integer load2 = (Integer) databaseMapEntry2.getValue();
			
			int weight1 = database1.getWeight().intValue();
			int weight2 = database2.getWeight().intValue();
			
			if (weight1 == weight2)
			{
				return load1.compareTo(load2);
			}
			
			float weightedLoad1 = (weight1 != 0) ? (load1.floatValue() / weight1) : Float.POSITIVE_INFINITY;
			float weightedLoad2 = (weight2 != 0) ? (load2.floatValue() / weight2) : Float.POSITIVE_INFINITY;
			
			return Float.compare(weightedLoad1, weightedLoad2);
		}
	};
	
	private Map databaseMap = new HashMap();
	
	/**
	 * @see net.sf.hajdbc.balancer.AbstractBalancer#getDatabases()
	 */
	protected Collection getDatabases()
	{
		return this.databaseMap.keySet();
	}

	/**
	 * @see net.sf.hajdbc.Balancer#add(net.sf.hajdbc.Database)
	 */
	public synchronized boolean add(Database database)
	{
		boolean exists = this.databaseMap.containsKey(database);
		
		if (!exists)
		{
			this.databaseMap.put(database, new Integer(1));
		}
		
		return !exists;
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#next()
	 */
	public synchronized Database next()
	{
		if (this.databaseMap.size() <= 1)
		{
			return this.first();
		}
		
		List databaseMapEntryList = new ArrayList(this.databaseMap.entrySet());
		
		Collections.sort(databaseMapEntryList, comparator);
		
		Map.Entry mapEntry = (Map.Entry) databaseMapEntryList.get(0);
		
		return (Database) mapEntry.getKey();
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#beforeOperation(net.sf.hajdbc.Database)
	 */
	public void beforeOperation(Database database)
	{
		this.incrementLoad(database, 1);
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#afterOperation(net.sf.hajdbc.Database)
	 */
	public void afterOperation(Database database)
	{
		this.incrementLoad(database, -1);
	}
	
	private synchronized void incrementLoad(Database database, int increment)
	{
		Integer load = (Integer) this.databaseMap.remove(database);

		this.databaseMap.put(database, new Integer(load.intValue() + increment));
	}
}
