/*
 * HA-JDBC: High-Availability JDBC
 * Copyright 2004-2009 Paul Ferraro
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.sf.hajdbc.balancer.simple;

import java.util.Collections;
import java.util.Comparator;
import java.util.Set;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.balancer.AbstractSetBalancer;

/**
 * Trivial balancer implementation whose {@link #next} implementation always returns the database with the highest weight.
 * 
 * @author  Paul Ferraro
 * @param <D> either java.sql.Driver or javax.sql.DataSource
 */
public class SimpleBalancer<Z, D extends Database<Z>> extends AbstractSetBalancer<Z, D>
{
	private volatile D nextDatabase = null;
	
	private Comparator<D> comparator = new Comparator<D>()
	{
		@Override
		public int compare(D database1, D database2)
		{
			return database1.getWeight() - database2.getWeight();
		}
	};

	/**
	 * Constructs a new SimpleBalancer
	 * @param databases
	 */
	public SimpleBalancer(Set<D> databases)
	{
		super(databases);
		
		this.reset();
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.balancer.Balancer#next()
	 */
	@Override
	public D next()
	{
		return this.nextDatabase;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.balancer.AbstractSetBalancer#added(net.sf.hajdbc.Database)
	 */
	@Override
	protected void added(D database)
	{
		this.reset();
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.balancer.AbstractSetBalancer#removed(net.sf.hajdbc.Database)
	 */
	@Override
	protected void removed(D database)
	{
		this.reset();
	}
	
	private void reset()
	{
		Set<D> databaseSet = this.getDatabases();
		
		this.nextDatabase = databaseSet.isEmpty() ? null : Collections.max(databaseSet, this.comparator);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.balancer.AbstractSetBalancer#cleared()
	 */
	@Override
	protected void cleared()
	{
		this.nextDatabase = null;
	}
}
