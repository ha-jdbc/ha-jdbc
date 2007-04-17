/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2007 Paul Ferraro
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
package net.sf.hajdbc.backport.java.lang.management;

import java.util.List;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;

/**
 * @author Paul Ferraro
 *
 */
public final class ManagementFactory
{
	public static synchronized MBeanServer getPlatformMBeanServer()
	{
		List serverList = MBeanServerFactory.findMBeanServer(null);
		
		return serverList.isEmpty() ? MBeanServerFactory.createMBeanServer() : (MBeanServer) serverList.get(0);
	}
}
