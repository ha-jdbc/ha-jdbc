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

import java.util.Collection;
import java.util.Map;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public abstract class DatabaseClusterDecorator extends DatabaseCluster
{
	protected DatabaseCluster databaseCluster;

	protected DatabaseClusterDecorator(DatabaseCluster databaseCluster)
	{
		this.databaseCluster = databaseCluster;
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#getId()
	 */
	public final String getId()
	{
		return this.databaseCluster.getId();
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#isActive(net.sf.hajdbc.Database)
	 */
	public boolean isActive(Database database)
	{
		return this.databaseCluster.isActive(database);
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#firstDatabase()
	 */
	public final Database firstDatabase() throws java.sql.SQLException
	{
		return this.databaseCluster.firstDatabase();
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getDatabases()
	 */
	public final Database[] getDatabases() throws java.sql.SQLException
	{
		return this.databaseCluster.getDatabases();
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getConnectionFactory()
	 */
	public final ConnectionFactoryProxy getConnectionFactory()
	{
		return this.databaseCluster.getConnectionFactory();
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#isAlive(net.sf.hajdbc.Database)
	 */
	public final boolean isAlive(Database database)
	{
		return this.databaseCluster.isAlive(database);
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#nextDatabase()
	 */
	public final Database nextDatabase() throws java.sql.SQLException
	{
		return this.databaseCluster.nextDatabase();
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getDatabase(java.lang.String)
	 */
	public final Database getDatabase(String databaseId) throws java.sql.SQLException
	{
		return this.databaseCluster.getDatabase(databaseId);
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#getActiveDatabases()
	 */
	public final Collection getActiveDatabases()
	{
		return this.databaseCluster.getActiveDatabases();
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#getInactiveDatabases()
	 */
	public final Collection getInactiveDatabases()
	{
		return this.databaseCluster.getInactiveDatabases();
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#getSynchronizationStrategies()
	 */
	public final Collection getSynchronizationStrategies()
	{
		return this.databaseCluster.getSynchronizationStrategies();
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getSynchronizationStrategy(java.lang.String)
	 */
	public final SynchronizationStrategy getSynchronizationStrategy(String id) throws java.sql.SQLException
	{
		return this.databaseCluster.getSynchronizationStrategy(id);
	}
	
	protected Map getConnectionFactoryMap()
	{
		return this.databaseCluster.getConnectionFactoryMap();
	}
	
	protected Balancer getBalancer()
	{
		return this.databaseCluster.getBalancer();
	}
}
