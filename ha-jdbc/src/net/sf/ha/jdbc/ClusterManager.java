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
public class ClusterManager
{
	private Set listenerSet = Collections.synchronizedSet(new HashSet());
	private Map clusterMap;
	
	protected ClusterManager()
	{
		// Initialize cluster
	}
	
	public Set getCluster(String name)
	{
		return (Set) this.clusterMap.get(name);
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
