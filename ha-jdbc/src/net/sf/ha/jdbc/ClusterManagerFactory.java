package net.sf.ha.jdbc;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class ClusterManagerFactory
{
	private static final boolean DISTRIBUTED = false;
	private static MBeanServer server;
	
	public static synchronized ClusterManager getClusterManager()
	{
		if (DISTRIBUTED)
		{
			if (server == null)
			{
				server = MBeanServerFactory.newMBeanServer();
			}
			
			try
			{
				return (ClusterManager) MBeanServerInvocationHandler.newProxyInstance(server, ObjectName.getInstance(""), DistributedClusterManagerMBean.class, false);
			}
			catch (JMException e)
			{
				throw new RuntimeException(e);
			}
		}
		
		return SimpleClusterManager.getInstance();
	}
}
