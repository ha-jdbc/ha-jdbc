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
import java.util.Collections;
import java.util.List;
import java.util.Random;

import net.sf.hajdbc.Database;

/**
 * Balancer implementation whose {@link #next()} implementation returns a random database.
 * The probability that a given database will be returned is: <em>weight / total-weight</em>.
 * 
 * @author  Paul Ferraro
 * @param <D> either java.sql.Driver or javax.sql.DataSource
 */
public class RandomBalancer<D> extends AbstractBalancer<D>
{
	private volatile List<Database<D>> databaseList = Collections.emptyList();

	private Random random = new Random();

	/**
	 * @see net.sf.hajdbc.Balancer#next()
	 */
	@Override
	public Database<D> next()
	{
		List<Database<D>> list = this.databaseList;
		
		if (list.isEmpty())
		{
			return this.databaseSet.first();
		}
		
		int index = this.random.nextInt(list.size());
		
		return list.get(index);
	}
	
	/**
	 * @see net.sf.hajdbc.balancer.AbstractBalancer#added(net.sf.hajdbc.Database)
	 */
	@Override
	protected void added(Database<D> database)
	{
		int weight = database.getWeight();
		
		if (weight > 0)
		{
			List<Database<D>> list = new ArrayList<Database<D>>(this.databaseList.size() + weight);
			
			list.addAll(this.databaseList);
			
			for (int i = 0; i < weight; ++i)
			{
				list.add(database);
			}
			
			this.databaseList = list;
		}
	}

	/**
	 * @see net.sf.hajdbc.balancer.AbstractBalancer#removed(net.sf.hajdbc.Database)
	 */
	@Override
	protected void removed(Database<D> database)
	{
		int weight = database.getWeight();

		if (weight > 0)
		{
			List<Database<D>> list = new ArrayList<Database<D>>(this.databaseList.size() - weight);
			
			int index = this.databaseList.indexOf(database);
			
			list.addAll(this.databaseList.subList(0, index));
			list.addAll(this.databaseList.subList(index + weight, this.databaseList.size()));
			
			this.databaseList = list;
		}
	}

	/**
	 * @see net.sf.hajdbc.balancer.AbstractBalancer#cleared()
	 */
	@Override
	protected void cleared()
	{
		this.databaseList = Collections.emptyList();
	}
}
