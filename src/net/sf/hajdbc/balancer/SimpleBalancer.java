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

import java.util.Collections;
import java.util.Comparator;
import java.util.NoSuchElementException;

import net.sf.hajdbc.Database;

/**
 * Trivial balancer implementation whose {@link #next} implementation always returns the database with the highest weight.
 * 
 * @author  Paul Ferraro
 * @param <D> either java.sql.Driver or javax.sql.DataSource
 */
public class SimpleBalancer<D> extends AbstractBalancer<D>
{
	private volatile Database<D> nextDatabase = null;
	
	private Comparator<Database<D>> comparator = new Comparator<Database<D>>()
	{
		@Override
		public int compare(Database<D> database1, Database<D> database2)
		{
			return database1.getWeight() - database2.getWeight();
		}
	};

	/**
	 * @see net.sf.hajdbc.Balancer#next()
	 */
	@Override
	public Database<D> next()
	{
		Database<D> next = this.nextDatabase;
		
		if (next == null)
		{
			throw new NoSuchElementException();
		}
		
		return next;
	}

	/**
	 * @see net.sf.hajdbc.balancer.AbstractBalancer#added(net.sf.hajdbc.Database)
	 */
	@Override
	protected void added(Database<D> database)
	{
		this.reset();
	}

	/**
	 * @see net.sf.hajdbc.balancer.AbstractBalancer#removed(net.sf.hajdbc.Database)
	 */
	@Override
	protected void removed(Database<D> database)
	{
		this.reset();
	}
	
	private void reset()
	{
		this.nextDatabase = Collections.max(this.databaseSet, this.comparator);
	}

	/**
	 * @see net.sf.hajdbc.balancer.AbstractBalancer#cleared()
	 */
	@Override
	protected void cleared()
	{
		this.nextDatabase = null;
	}
}
