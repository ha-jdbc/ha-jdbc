/*
 * Copyright (c) 2004, Identity Theft 911, LLC.  All rights reserved.
 *
 * $Id$
 */
package net.sf.hajdbc.distributable;

import java.io.Serializable;
import java.sql.SQLException;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseClusterDescriptor;
import net.sf.hajdbc.DatabaseClusterFactory;
import net.sf.hajdbc.DatabaseClusterListener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.blocks.NotificationBus;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class DistributableDatabaseClusterListener implements DatabaseClusterListener, NotificationBus.Consumer
{
	private static Log log = LogFactory.getLog(DistributableDatabaseClusterListener.class);
	
	private String databaseClusterName;
	private NotificationBus notificationBus;
	
	public DistributableDatabaseClusterListener(DatabaseClusterDescriptor descriptor, String protocol) throws Exception
	{
		this.databaseClusterName = descriptor.getName();
		this.notificationBus = new NotificationBus(this.databaseClusterName, protocol);
		this.notificationBus.setConsumer(this);
		this.notificationBus.start();
	}
	
	/**
	 * @see org.jgroups.blocks.NotificationBus.Consumer#handleNotification(java.io.Serializable)
	 */
	public void handleNotification(Serializable object)
	{
		DatabaseCommand command = (DatabaseCommand) object;
		
		try
		{
			DatabaseCluster databaseCluster = DatabaseClusterFactory.getInstance().getDatabaseCluster(this.databaseClusterName);
			
			command.execute(databaseCluster);
		}
		catch (SQLException e)
		{
			log.error("Failed to execute " + command + " on database cluster " + this.databaseClusterName, e);
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
	protected void finalize()
	{
		this.notificationBus.stop();
	}
	
	public void deactivated(Database database)
	{
		this.notificationBus.sendNotification(new DatabaseDeactivationCommand(database.getId()));
	}
}
