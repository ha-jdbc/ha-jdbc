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

import java.util.Collection;

import EDU.oswego.cs.dl.util.concurrent.ReadWriteLock;
import EDU.oswego.cs.dl.util.concurrent.Sync;
import EDU.oswego.cs.dl.util.concurrent.WriterPreferenceReadWriteLock;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.Operation;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public abstract class AbstractBalancer implements Balancer
{
	protected abstract Collection getDatabases();
	
	private ReadWriteLock lock = new WriterPreferenceReadWriteLock();
	
	protected Sync acquireReadLock()
	{
		return this.acquire(this.lock.readLock());
	}

	protected Sync acquireWriteLock()
	{
		return this.acquire(this.lock.writeLock());
	}
	
	private Sync acquire(Sync lock)
	{
		try
		{
			lock.acquire();
			
			return lock;
		}
		catch (InterruptedException e)
		{
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#execute(net.sf.hajdbc.Operation, net.sf.hajdbc.Database, java.lang.Object)
	 */
	public Object execute(Operation operation, Database database, Object object) throws java.sql.SQLException
	{
		this.beforeOperation(database);
		
		try
		{
			return operation.execute(database, object);
		}
		finally
		{
			this.afterOperation(database);
		}
	}
	
	/**
	 * Called before an operation is performed on the specified database.
	 * @param database a database descriptor
	 */
	protected void beforeOperation(Database database)
	{
		// Do nothing
	}
	
	/**
	 * Called after an operation is performed on the specified database.
	 * @param database a database descriptor
	 */
	protected void afterOperation(Database database)
	{
		// Do nothing
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#remove(net.sf.hajdbc.Database)
	 */
	public boolean remove(Database database)
	{
		Sync lock = this.acquireWriteLock();
		
		try
		{
			return this.getDatabases().remove(database);
		}
		finally
		{
			lock.release();
		}
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#add(net.sf.hajdbc.Database)
	 */
	public boolean add(Database database)
	{
		Sync lock = this.acquireWriteLock();
		
		try
		{
			return (this.contains(database)) ? this.getDatabases().add(database) : false;
		}
		finally
		{
			lock.release();
		}
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#toArray()
	 */
	public Database[] toArray()
	{
		Sync lock = this.acquireReadLock();
		
		try
		{
			Collection databases = getDatabases();
			
			return (Database[]) databases.toArray(new Database[databases.size()]);
		}
		finally
		{
			lock.release();
		}
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#contains(net.sf.hajdbc.Database)
	 */
	public boolean contains(Database database)
	{
		Sync lock = this.acquireReadLock();
		
		try
		{
			return this.getDatabases().contains(database);
		}
		finally
		{
			lock.release();
		}
	}
	
	/**
	 * @see net.sf.hajdbc.Balancer#first()
	 */
	public Database first()
	{
		Sync lock = this.acquireReadLock();
		
		try
		{
			return (Database) this.getDatabases().iterator().next();
		}
		finally
		{
			lock.release();
		}
	}
}
