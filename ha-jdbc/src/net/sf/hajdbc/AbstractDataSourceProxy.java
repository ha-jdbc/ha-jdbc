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
import java.util.Iterator;
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

	/**
	 * @return
	 */
	public String getName()
	{
		return this.databaseCluster.getName();
	}
	
	public void setName(String name) throws SQLException, NamingException
	{
		DatabaseClusterManager manager = DatabaseClusterManagerFactory.getDatabaseClusterManager();
		DatabaseClusterDescriptor descriptor = manager.getDescriptor(name);
		Map databaseMap = descriptor.getDatabaseMap();
		Map dataSourceMap = new HashMap(databaseMap.size());
		Context context = new InitialContext();
		
		Iterator databases = databaseMap.values().iterator();
		
		while (databases.hasNext())
		{
			DataSourceDatabase database = (DataSourceDatabase) databases.next();
			Object dataSource = context.lookup(database.getName());
			
			dataSourceMap.put(database, dataSource);
		}
		
		this.databaseCluster = new DatabaseCluster(descriptor, dataSourceMap);
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
