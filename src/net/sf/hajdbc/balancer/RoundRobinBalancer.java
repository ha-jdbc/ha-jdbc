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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import net.sf.hajdbc.Database;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public class RoundRobinBalancer extends AbstractBalancer
{
	private Set<Database> databaseSet = new HashSet<Database>();
	private Queue<Database> databaseQueue = new LinkedList<Database>();

	/**
	 * @see net.sf.hajdbc.balancer.AbstractBalancer#collect()
	 */
	@Override
	protected Collection<Database> collect()
	{
		return this.databaseSet;
	}

	/**
	 * @see net.sf.hajdbc.Balancer#add(net.sf.hajdbc.Database)
	 */
	@Override
	public synchronized boolean add(Database database)
	{
		boolean added = super.add(database);
		
		if (added)
		{
			int weight = database.getWeight();
			
			for (int i = 0; i < weight; ++i)
			{
				this.databaseQueue.add(database);
			}
		}
		
		return added;
	}

	/**
	 * @see net.sf.hajdbc.Balancer#remove(net.sf.hajdbc.Database)
	 */
	@Override
	public synchronized boolean remove(Database database)
	{
		boolean removed = super.remove(database);

		if (removed)
		{
			int weight = database.getWeight();

			for (int i = 0; i < weight; ++i)
			{
				this.databaseQueue.remove(database);
			}
		}
		
		return removed;
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#first()
	 */
	@Override
	public synchronized Database first()
	{
		return (this.databaseQueue.isEmpty()) ? super.first() : this.databaseQueue.element();
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#next()
	 */
	public synchronized Database next()
	{
		if (this.databaseQueue.isEmpty())
		{
			return super.first();
		}
		
		Database database = this.databaseQueue.remove();
		
		this.databaseQueue.add(database);
		
		return database;
	}
}
