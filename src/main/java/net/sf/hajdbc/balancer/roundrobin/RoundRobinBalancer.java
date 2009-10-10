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
package net.sf.hajdbc.balancer.roundrobin;

import java.util.LinkedList;
import java.util.Queue;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.balancer.AbstractSetBalancer;

/**
 * Balancer implementation whose {@link #next()} implementation uses a circular FIFO queue.
 * 
 * @author  Paul Ferraro
 * @param <D> either java.sql.Driver or javax.sql.DataSource
 */
public class RoundRobinBalancer<P, D extends Database<P>> extends AbstractSetBalancer<P, D>
{
	private Queue<D> databaseQueue = new LinkedList<D>();

	/**
	 * @see net.sf.hajdbc.balancer.AbstractBalancer#added(net.sf.hajdbc.Database)
	 */
	@Override
	protected void added(D database)
	{
		int weight = database.getWeight();
		
		for (int i = 0; i < weight; ++i)
		{
			this.databaseQueue.add(database);
		}
	}

	/**
	 * @see net.sf.hajdbc.balancer.AbstractBalancer#removed(net.sf.hajdbc.Database)
	 */
	@Override
	protected void removed(D database)
	{
		int weight = database.getWeight();
		
		for (int i = 0; i < weight; ++i)
		{
			this.databaseQueue.remove(database);
		}
	}
	
	/**
	 * @see net.sf.hajdbc.balancer.Balancer#next()
	 */
	@Override
	public D next()
	{
		this.getLock().lock();
		
		try
		{
			if (this.databaseQueue.isEmpty())
			{
				return null;
			}
			
			if (this.databaseQueue.size() == 1)
			{
				return this.databaseQueue.element();
			}
			
			D database = this.databaseQueue.remove();
			
			this.databaseQueue.add(database);
			
			return database;
		}
		finally
		{
			this.getLock().unlock();
		}
	}

	/**
	 * @see net.sf.hajdbc.balancer.AbstractBalancer#cleared()
	 */
	@Override
	protected void cleared()
	{
		this.databaseQueue.clear();
	}
}
