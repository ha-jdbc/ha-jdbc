package net.sf.ha.jdbc;

import java.util.Set;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public interface DatabaseClusterManager
{
	public DatabaseClusterDescriptor getDescriptor(String name);
	
	public Set getClusterSet(Class databaseClass);
	
	public void deactivate(String clusterName, Database database);
}
