package net.sf.ha.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jibx.runtime.BindingDirectory;
import org.jibx.runtime.IBindingFactory;
import org.jibx.runtime.IUnmarshallingContext;
import org.jibx.runtime.JiBXException;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class DatabaseClusterManagerFactory
{
	private static Log log = LogFactory.getLog(DatabaseClusterManagerFactory.class);
	private static final boolean DISTRIBUTED = false;
	private static MBeanServer server;
	
	public static synchronized DatabaseClusterManager getClusterManager()
	{
		URL url = DatabaseClusterManagerFactory.class.getResource("ha-jdbc.xml");
		InputStream inputStream = null;
		
		try
		{
			inputStream = url.openStream();
			IBindingFactory factory = BindingDirectory.getFactory(DatabaseClusterManager.class);
			IUnmarshallingContext context = factory.createUnmarshallingContext();
			Reader reader = new InputStreamReader(inputStream);
			DatabaseClusterManager manager = (DatabaseClusterManager) context.unmarshalDocument(reader);
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		catch (JiBXException e)
		{
			e.printStackTrace();
		}
		finally
		{
			if (inputStream != null)
			{
				try
				{
					inputStream.close();
				}
				catch (IOException e)
				{
					log.warn("Failed to close " + url, e);
				}
			}
		}
		
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
