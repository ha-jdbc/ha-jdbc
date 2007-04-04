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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import net.sf.hajdbc.Database;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public class RandomBalancer<D> extends AbstractBalancer<D>
{
	private Random random = new Random();
	private SortedSet<Database<D>> databaseSet = new TreeSet<Database<D>>();
	private List<Database<D>> databaseList = new ArrayList<Database<D>>();
	
	/**
	 * @see net.sf.hajdbc.balancer.AbstractBalancer#collect()
	 */
	@Override
	protected Collection<Database<D>> collect()
	{
		return this.databaseSet;
	}

	/**
	 * @see net.sf.hajdbc.Balancer#add(net.sf.hajdbc.Database)
	 */
	@Override
	public synchronized boolean add(Database<D> database)
	{
		boolean added = super.add(database);
		
		if (added)
		{
			int weight = database.getWeight();
			
			for (int i = 0; i < weight; ++i)
			{
				this.databaseList.add(database);
			}
		}
		
		return added;
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#remove(net.sf.hajdbc.Database)
	 */
	@Override
	public synchronized boolean remove(Database<D> database)
	{
		boolean removed = super.remove(database);
		
		int weight = database.getWeight();
		
		if (removed && (weight > 0))
		{
			int index = this.databaseList.indexOf(database);
			
			this.databaseList.subList(index, index + weight).clear();
		}
		
		return removed;
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#next()
	 */
	public synchronized Database<D> next()
	{
		if (this.databaseList.isEmpty())
		{
			return this.databaseSet.first();
		}
		
		int index = this.random.nextInt(this.databaseList.size());
		
		return this.databaseList.get(index);
	}
}
