package net.sf.ha.jdbc;

import java.util.Collections;
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
	private Set listenerSet = Collections.synchronizedSet(new HashSet());
	// Maps cluster name -> Set of Databases
	private Map databaseClusterMap;
	// Maps cluster type -> Set of cluster names
	private Map classMap;
	
	protected DatabaseClusterManager()
	{
		// Initialize cluster
	}
	
	public Set getDatabaseSet(String name)
	{
		return (Set) this.databaseClusterMap.get(name);
	}
	
	public Set getClusterSet(Class databaseClass)
	{
		return (Set) this.classMap.get(databaseClass);
	}
	
	public void addDatabaseEventListener(DatabaseEventListener listener)
	{
		this.listenerSet.add(listener);
	}
	
	public void removeDatabaseEventListener(DatabaseEventListener listener)
	{
		this.listenerSet.remove(listener);
	}
}
