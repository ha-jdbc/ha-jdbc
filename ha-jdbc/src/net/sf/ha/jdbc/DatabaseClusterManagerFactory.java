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
public class DatabaseClusterManagerFactory
{
	private static final boolean DISTRIBUTED = false;
	private static MBeanServer server;
	
	public static synchronized DatabaseClusterManager getClusterManager()
	{
		if (DISTRIBUTED)
		{
			if (server == null)
			{
				server = MBeanServerFactory.newMBeanServer();
			}
			
			try
			{
				return (DatabaseClusterManager) MBeanServerInvocationHandler.newProxyInstance(server, ObjectName.getInstance(""), DistributedClusterManagerMBean.class, false);
			}
			catch (JMException e)
			{
				throw new RuntimeException(e);
			}
		}
		
		return SimpleClusterManager.getInstance();
	}
}
