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
package net.sf.hajdbc.distributable;

import java.io.Serializable;
import java.sql.SQLException;

import net.sf.hajdbc.DatabaseClusterDecorator;
import net.sf.hajdbc.DatabaseClusterMBean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.blocks.NotificationBus;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class DistributableDatabaseCluster extends DatabaseClusterDecorator implements NotificationBus.Consumer
{
	private static Log log = LogFactory.getLog(DistributableDatabaseCluster.class);
	
	private NotificationBus notificationBus;
	
	public DistributableDatabaseCluster(DatabaseClusterMBean databaseCluster, DistributableDatabaseClusterDescriptor descriptor) throws Exception
	{
		super(databaseCluster);
		
		this.notificationBus = new NotificationBus(databaseCluster.getName(), descriptor.getProtocol());
		this.notificationBus.setConsumer(this);
		this.notificationBus.start();
	}

	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#deactivate(java.lang.String)
	 */
	public void deactivate(String databaseId)
	{
		this.databaseCluster.deactivate(databaseId);
		
		this.notificationBus.sendNotification(new DatabaseDeactivationCommand(databaseId));
	}

	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#activate(java.lang.String, java.lang.String)
	 */
	public void activate(String databaseId, String strategyClassName) throws SQLException
	{
		this.databaseCluster.activate(databaseId, strategyClassName);
		
		this.notificationBus.sendNotification(new DatabaseActivationCommand(databaseId));
	}

	/**
	 * @see org.jgroups.blocks.NotificationBus.Consumer#handleNotification(java.io.Serializable)
	 */
	public void handleNotification(Serializable object)
	{
		DatabaseCommand command = (DatabaseCommand) object;
		
		try
		{
			command.execute(this.databaseCluster);
		}
		catch (SQLException e)
		{
			log.error("Failed to execute " + command + " on database cluster " + this.databaseCluster.getName(), e);
		}
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
		log.info(address + " joined " + this.notificationBus.getChannel().getChannelName() + " channel.");
	}

	/**
	 * @see org.jgroups.blocks.NotificationBus.Consumer#memberLeft(org.jgroups.Address)
	 */
	public void memberLeft(Address address)
	{
		log.info(address + " left " + this.notificationBus.getChannel().getChannelName() + " channel.");
	}
	
	/**
	 * @see java.lang.Object#finalize()
	 */
	protected void finalize() throws Throwable
	{
		this.notificationBus.stop();
		
		super.finalize();
	}
}
