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
package net.sf.hajdbc;

import java.util.List;

/**
 * Contains a map of <code>Database</code> -&gt; database connection factory (i.e. Driver, DataSource, ConnectionPoolDataSource, XADataSource)
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public abstract class DatabaseCluster implements DatabaseClusterMBean
{
	public abstract DatabaseConnector getDatabaseConnector();
	
	public abstract boolean isActive(Database database);
	
	public abstract boolean deactivate(Database database);

	public abstract DatabaseClusterDescriptor getDescriptor();
	
	protected abstract Database getDatabase(String databaseId);
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#isActive(java.lang.String)
	 */
	public boolean isActive(String databaseId)
	{
		return this.isActive(this.getDatabase(databaseId));
	}

	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#deactivate(java.lang.String)
	 */
	public void deactivate(String databaseId)
	{
		this.deactivate(this.getDatabase(databaseId));
	}

	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#activate(java.lang.String, java.lang.String)
	 */
	public void activate(String databaseId, String strategyClassName) throws java.sql.SQLException
	{
		Database database = this.getDatabase(databaseId);
		
		try
		{
			Class strategyClass = Class.forName(strategyClassName);
			
			if (!DatabaseSynchronizationStrategy.class.isAssignableFrom(strategyClass))
			{
				throw new SQLException("Specified synchronization strategy does not implement " + DatabaseSynchronizationStrategy.class.getName());
			}
			
			DatabaseSynchronizationStrategy strategy = (DatabaseSynchronizationStrategy) strategyClass.newInstance();
			
			strategy.synchronize(this, database);
			
			this.activate(database);
		}
		catch (ClassNotFoundException e)
		{
			throw new SQLException(e);
		}
		catch (InstantiationException e)
		{
			throw new SQLException(e);
		}
		catch (IllegalAccessException e)
		{
			throw new SQLException(e);
		}
	}
	
	public void activate(String databaseId)
	{
		this.activate(this.getDatabase(databaseId));
	}
	
	public abstract boolean activate(Database database);
	
	public abstract Database firstDatabase() throws java.sql.SQLException;
	
	public abstract Database nextDatabase() throws java.sql.SQLException;

	public abstract List getActiveDatabaseList() throws java.sql.SQLException;
}
