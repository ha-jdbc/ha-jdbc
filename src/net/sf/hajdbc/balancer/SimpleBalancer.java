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

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;

import net.sf.hajdbc.Database;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public class SimpleBalancer<D> extends AbstractBalancer<D>
{
	private LinkedList<Database<D>> databaseList = new LinkedList<Database<D>>();

	/**
	 * @see net.sf.hajdbc.balancer.AbstractBalancer#collect()
	 */
	@Override
	protected Collection<Database<D>> collect()
	{
		return this.databaseList;
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#next()
	 */
	@Override
	public synchronized Database<D> next()
	{
		return this.databaseList.element();
	}
	
	/**
	 * @see net.sf.hajdbc.balancer.AbstractBalancer#add(net.sf.hajdbc.Database)
	 */
	@Override
	public synchronized boolean add(Database<D> database)
	{
		boolean added = super.add(database);
		
		if (added)
		{
			Collections.sort(this.databaseList, this.comparator);
		}
		
		return added;
	}

	private Comparator<Database<D>> comparator = new Comparator<Database<D>>()
	{
		/**
		 * @see java.util.Comparator#compare(T, T)
		 */
		public int compare(Database<D> database1, Database<D> database2)
		{
			return database2.getWeight() - database1.getWeight();
		}
	};
}
