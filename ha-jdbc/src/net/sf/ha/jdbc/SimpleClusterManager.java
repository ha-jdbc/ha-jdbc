package net.sf.ha.jdbc;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class SimpleClusterManager extends ClusterManager
{
	public static ClusterManager instance = new SimpleClusterManager();
	
	private SimpleClusterManager()
	{
		super();
	}
	
	public static ClusterManager getInstance()
	{
		return instance;
	}
}
