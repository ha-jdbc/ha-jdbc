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
package net.sf.hajdbc.local;

import java.util.HashMap;
import java.util.Map;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseClusterDescriptor;
import net.sf.hajdbc.SynchronizationStrategy;
import net.sf.hajdbc.SynchronizationStrategyDescriptor;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class LocalDatabaseClusterDescriptor implements DatabaseClusterDescriptor
{
	private String id;
	private String validateSQL;
	private Map databaseMap = new HashMap();
	private Map synchronizationStrategyMap = new HashMap();
	
	/**
	 * Returns a mapping of database id to Database object.
	 * @return a map of databases in this cluster
	 */
	public Map getDatabaseMap()
	{
		return this.databaseMap;
	}
	
	/**
	 * Adds the specified database to this cluster.
	 * This method is only used by JiBX.
	 * @param object a Database object
	 */
	void addDatabase(Object object)
	{
		Database database = (Database) object;
		
		this.databaseMap.put(database.getId(), database);
	}
	
	/**
	 * Adds the specified synchronization strategy descriptor to this cluster.
	 * This method is only used by JiBX.
	 * @param object a SynchronizationStrategyDescriptor
	 * @throws Exception if synchronization strategy could not be created
	 */
	void addSynchronizationStrategy(Object object) throws Exception
	{
		SynchronizationStrategyDescriptor descriptor = (SynchronizationStrategyDescriptor) object;
		
		SynchronizationStrategy strategy = descriptor.createSynchronizationStrategy();
		
		this.synchronizationStrategyMap.put(descriptor.getId(), strategy);
	}
	
	/**
	 * Returns the identifier of this database cluster.
	 * @return a database cluster identifier
	 */
	public String getId()
	{
		return this.id;
	}
	
	/**
	 * Returns the SQL statement used to validate whether or not a database is responding.
	 * @return a SQL statement
	 */
	public String getValidateSQL()
	{
		return this.validateSQL;
	}
	
	/**
	 * Returns a map of synchronization strategies for this cluster. 
	 * @return a Map<String, SynchronizationStrategy>
	 */
	public Map getSynchronizationStrategyMap()
	{
		return this.synchronizationStrategyMap;
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterDescriptor#createDatabaseCluster()
	 */
	public DatabaseCluster createDatabaseCluster() throws java.sql.SQLException
	{
		return new LocalDatabaseCluster(this);
	}
}
