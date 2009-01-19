/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2008 Paul Ferraro
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
package net.sf.hajdbc.util.servlet;

import java.lang.management.ManagementFactory;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import net.sf.hajdbc.DatabaseClusterFactory;

/**
 * Utility to automatically shutdown a list of database clusters scoped to a servlet context.
 * The clusters to shutdown are defined in a comma delimited init parameter: <code>ha-jdbc.clusters</code>
 * 
 * @author Paul Ferraro
 */
public class DatabaseClusterShutdownContextListener implements ServletContextListener
{
	private static final String CLUSTERS = "ha-jdbc.clusters";
	private static final String DELIMITER = ",";
	
	/**
	 * @see javax.servlet.ServletContextListener#contextDestroyed(javax.servlet.ServletContextEvent)
	 */
	@Override
	public void contextDestroyed(ServletContextEvent event)
	{
		ServletContext context = event.getServletContext();
		
		String clusters = context.getInitParameter(CLUSTERS);
		
		if (clusters != null)
		{
			MBeanServer server = ManagementFactory.getPlatformMBeanServer();
			
			for (String cluster: clusters.split(DELIMITER))
			{
				try
				{
					ObjectName name = DatabaseClusterFactory.getObjectName(cluster.trim());
					
					if (server.isRegistered(name))
					{
						server.unregisterMBean(name);
					}
				}
				catch (JMException e)
				{
					event.getServletContext().log(e.getMessage(), e);
				}
			}
		}
	}

	/**
	 * @see javax.servlet.ServletContextListener#contextInitialized(javax.servlet.ServletContextEvent)
	 */
	@Override
	public void contextInitialized(ServletContextEvent event)
	{
		// Do nothing
	}
}
