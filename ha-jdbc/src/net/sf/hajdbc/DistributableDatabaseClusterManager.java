/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2004 Paul Ferraro
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
package net.sf.hajdbc;

import java.io.Serializable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.blocks.NotificationBus;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class DistributableDatabaseClusterManager extends LocalDatabaseClusterManager implements NotificationBus.Consumer
{
	private static final String BUS_NAME = "DatabaseClusterManager";
	private static Log log = LogFactory.getLog(DistributableDatabaseClusterManager.class);
	
	private NotificationBus notificationBus;
	
	public void setProtocol(String protocol) throws Exception
	{
		this.notificationBus = new NotificationBus(BUS_NAME, protocol);
		this.notificationBus.setConsumer(this);
		this.notificationBus.start();
	}
	
	protected void finalize()
	{
		if (this.notificationBus != null)
		{
			this.notificationBus.stop();
		}
	}
	
	public boolean deactivate(DatabaseCluster databaseCluster, Database database)
	{
		String clusterName = databaseCluster.getDescriptor().getName();
		
		boolean deativated = super.deactivate(clusterName, database);
		
		if (deativated)
		{
			this.notificationBus.sendNotification(new String[] { clusterName, database.getId() });
		}
		
		return deativated;
	}

	/**
	 * @see org.jgroups.blocks.NotificationBus.Consumer#handleNotification(java.io.Serializable)
	 */
	public void handleNotification(Serializable object)
	{
		String[] notification = (String[]) object;
		String clusterName = notification[0];
		String databaseId = notification[1];
		
		Database database = (Database) this.getDescriptor(clusterName).getDatabaseMap().get(databaseId);
		
		super.deactivate(clusterName, database);
	}

	/**
	 * @see org.jgroups.blocks.NotificationBus.Consumer#getCache()
	 */
	public Serializable getCache()
	{
		return null;
	}

	/**
	 * @see org.jgroups.blocks.NotificationBus.Consumer#memberJoined(org.jgroups.Address)
	 */
	public void memberJoined(Address address)
	{
		log.info("Member joined: " + address);
	}

	/**
	 * @see org.jgroups.blocks.NotificationBus.Consumer#memberLeft(org.jgroups.Address)
	 */
	public void memberLeft(Address address)
	{
		log.info("Member left: " + address);
	}
}
