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
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import net.sf.hajdbc.Database;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public class SimpleBalancer extends AbstractBalancer
{
	private static Comparator<Database> comparator = new Comparator<Database>()
	{
		public int compare(Database database1, Database database2)
		{
			return database2.getWeight().compareTo(database1.getWeight());
		}
	};
		
	private List databaseList = new LinkedList();

	/**
	 * @see net.sf.hajdbc.balancer.AbstractBalancer#getDatabases()
	 */
	protected Collection<Database> getDatabases()
	{
		return this.databaseList;
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#add(net.sf.hajdbc.Database)
	 */
	public synchronized boolean add(Database database)
	{
		boolean exists = this.databaseList.contains(database);
		
		if (!exists)
		{
			this.databaseList.add(database);
			
			Collections.sort(this.databaseList, comparator);
		}
		
		return !exists;
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#first()
	 */
	public synchronized Database first()
	{
		if (this.databaseList.isEmpty())
		{
			throw new NoSuchElementException();
		}
		
		return (Database) this.databaseList.get(0);
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#next()
	 */
	public synchronized Database next()
	{
		return this.first();
	}
}
