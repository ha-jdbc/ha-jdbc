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
import java.util.Set;
import java.util.TreeSet;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.Database;

/**
 * Abstract balancer implementation that implements most of the Balancer interface.
 * 
 * @author  Paul Ferraro
 * @since   1.0
 */
public abstract class AbstractBalancer<D> implements Balancer<D>
{
	/**
	 * @see net.sf.hajdbc.Balancer#beforeInvocation(net.sf.hajdbc.Database)
	 */
	public void beforeInvocation(Database<D> database)
	{
		// Do nothing
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#afterInvocation(net.sf.hajdbc.Database)
	 */
	public void afterInvocation(Database<D> database)
	{
		// Do nothing
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#remove(net.sf.hajdbc.Database)
	 */
	public synchronized boolean remove(Database<D> database)
	{
		return this.collect().remove(database);
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#add(net.sf.hajdbc.Database)
	 */
	public synchronized boolean add(Database<D> database)
	{
		return this.collect().contains(database) ? false : this.collect().add(database);
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#contains(net.sf.hajdbc.Database)
	 */
	public synchronized boolean contains(Database<D> database)
	{
		return this.collect().contains(database);
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#all()
	 */
	public synchronized Set<Database<D>> all()
	{
		Set<Database<D>> databaseSet = new TreeSet<Database<D>>(this.collect());
		
		return Collections.unmodifiableSet(databaseSet);
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#size()
	 */
	@Override
	public synchronized int size()
	{
		return this.collect().size();
	}

	/**
	 * Exposes a view of the underlying collection of Databases.
	 * @return a Collection of databases
	 */
	protected abstract Collection<Database<D>> collect();
}
