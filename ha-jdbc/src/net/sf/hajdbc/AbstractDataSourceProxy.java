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

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public abstract class AbstractDataSourceProxy implements Referenceable
{
	protected DatabaseCluster databaseCluster;

	public DatabaseCluster getDatabaseCluster()
	{
		return this.databaseCluster;
	}
	
	public void setDatabaseCluster(DatabaseCluster databaseCluster)
	{
		this.databaseCluster = databaseCluster;
	}

	public String getName()
	{
		return this.databaseCluster.getDescriptor().getName();
	}
	
	public void setName(String name) throws SQLException, NamingException
	{
		DatabaseClusterManager manager = DatabaseClusterManagerFactory.getClusterManager();
		DatabaseClusterDescriptor descriptor = manager.getDescriptor(name);
		List databaseList = descriptor.getActiveDatabaseList();
		Map dataSourceMap = new HashMap(databaseList.size());
		Context context = new InitialContext();
		
		for (int i = 0; i < databaseList.size(); ++i)
		{
			DataSourceDatabase database = (DataSourceDatabase) databaseList.get(i);
			Object dataSource = context.lookup(database.getName());
			
			dataSourceMap.put(database, dataSource);
		}
		
		this.databaseCluster = new DatabaseCluster(manager, descriptor, dataSourceMap);
	}
	
	/**
	 * @see javax.naming.Referenceable#getReference()
	 */
	public final Reference getReference()
	{
        Reference ref = new Reference(this.getClass().getName(), this.getObjectFactoryClass().getName(), null);
        
        ref.add(new StringRefAddr(AbstractDataSourceFactory.CLUSTER_NAME, this.getName()));
        
        return ref;
	}
	
	protected abstract Class getObjectFactoryClass();
}
