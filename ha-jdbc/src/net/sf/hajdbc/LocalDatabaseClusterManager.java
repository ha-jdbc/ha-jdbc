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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class LocalDatabaseClusterManager implements DatabaseClusterManager
{
	// Maps cluster name -> DatabaseClusterDescriptor
	private Map descriptorMap = new HashMap();
	// Maps cluster type -> Set of cluster names
	private Map classMap = new HashMap();
	
	public void addDescriptor(Object databaseClusterDescriptor)
	{
		DatabaseClusterDescriptor descriptor = (DatabaseClusterDescriptor) databaseClusterDescriptor;
		String clusterName = descriptor.getName();
		
		this.descriptorMap.put(clusterName, descriptor);
		
		Class databaseClass = descriptor.getDatabaseMap().values().iterator().next().getClass();
		Set nameSet = (Set) this.classMap.get(databaseClass);
		
		if (nameSet == null)
		{
			nameSet = new HashSet();
			this.classMap.put(databaseClass, nameSet);
		}
		
		nameSet.add(clusterName);
	}
	
	public DatabaseClusterDescriptor getDescriptor(String name)
	{
		return (DatabaseClusterDescriptor) this.descriptorMap.get(name);
	}
	
	public Set getClusterSet(Class databaseClass)
	{
		return (Set) this.classMap.get(databaseClass);
	}
	
	public boolean deactivate(String clusterName, Database database)
	{
		return false;
//		return this.getDescriptor(clusterName).removeDatabase(database);
	}
}
