package net.sf.ha.jdbc;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class DatabaseClusterManager
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
	
	public void deactivate(String clusterName, Database database)
	{
		this.getDescriptor(clusterName).removeDatabase(database);
	}
}
