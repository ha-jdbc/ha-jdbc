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
import java.util.List;
import java.util.Set;

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
	 * @see net.sf.hajdbc.DatabaseClusterMBean#getName()
	 */
	public final String getName()
	{
		return this.databaseCluster.getName();
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#isActive(net.sf.hajdbc.Database)
	 */
	public boolean isActive(Database database)
	{
		return this.databaseCluster.isActive(database);
	}
	
	public final Database firstDatabase() throws java.sql.SQLException
	{
		return this.databaseCluster.firstDatabase();
	}
	
	public final List getActiveDatabaseList() throws java.sql.SQLException
	{
		return this.databaseCluster.getActiveDatabaseList();
	}
	
	public final ConnectionFactoryProxy getConnectionFactory()
	{
		return this.databaseCluster.getConnectionFactory();
	}
	
	public final DatabaseClusterDescriptor getDescriptor()
	{
		return this.databaseCluster.getDescriptor();
	}
	
	public final boolean isAlive(Database database)
	{
		return this.databaseCluster.isAlive(database);
	}
	
	public final Database nextDatabase() throws java.sql.SQLException
	{
		return this.databaseCluster.nextDatabase();
	}
	
	public final boolean addDatabase(Database database)
	{
		return this.databaseCluster.addDatabase(database);
	}
	
	public final boolean removeDatabase(Database database)
	{
		return this.databaseCluster.removeDatabase(database);
	}
	
	public final Database getDatabase(String databaseId) throws java.sql.SQLException
	{
		return this.databaseCluster.getDatabase(databaseId);
	}
	
	public final Set getNewDatabaseSet(Collection databases)
	{
		return this.databaseCluster.getNewDatabaseSet(databases);
	}
}
