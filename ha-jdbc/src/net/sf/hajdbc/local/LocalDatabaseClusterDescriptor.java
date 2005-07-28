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

import java.util.LinkedList;
import java.util.List;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseClusterDescriptor;
import net.sf.hajdbc.SynchronizationStrategyDescriptor;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class LocalDatabaseClusterDescriptor implements DatabaseClusterDescriptor
{
	private String id;
	private Balancer balancer;
	private String validateSQL;
	private SynchronizationStrategyDescriptor defaultSynchronizationStrategy;
	private List databaseList = new LinkedList();
	
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
	 * Returns a balancer implementation
	 * @return the balancer implementation
	 */
	public Balancer getBalancer()
	{
		return this.balancer;
	}
	
	/**
	 * Returns a mapping of database id to Database object.
	 * @return a map of databases in this cluster
	 */
	public List getDatabaseList()
	{
		return this.databaseList;
	}
	
	/**
	 * Returns a map of synchronization strategies for this cluster. 
	 * @return a Map<String, SynchronizationStrategy>
	 */
	public SynchronizationStrategyDescriptor getDefaultSynchronizationStrategy()
	{
		return this.defaultSynchronizationStrategy;
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterDescriptor#createDatabaseCluster()
	 */
	public DatabaseCluster createDatabaseCluster() throws java.sql.SQLException
	{
		return new LocalDatabaseCluster(this);
	}
	
	void addDatabase(Object database)
	{
		this.databaseList.add(database);
	}
}
