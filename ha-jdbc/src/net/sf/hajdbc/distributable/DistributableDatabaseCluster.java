/*
 * Copyright (c) 2004, Identity Theft 911, LLC.  All rights reserved.
 *
 * $Id$
 */
package net.sf.hajdbc.distributable;

import java.io.Serializable;
import java.sql.SQLException;

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
public class DistributableDatabaseCluster implements DatabaseClusterMBean, NotificationBus.Consumer
{
	private static Log log = LogFactory.getLog(DistributableDatabaseCluster.class);
	
	private DatabaseClusterMBean databaseCluster;
	private NotificationBus notificationBus;
	
	public DistributableDatabaseCluster(DatabaseClusterMBean databaseCluster, String protocol) throws Exception
	{
		this.databaseCluster = databaseCluster;
		this.notificationBus = new NotificationBus(databaseCluster.getName(), protocol);
		this.notificationBus.setConsumer(this);
		this.notificationBus.start();
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#getName()
	 */
	public String getName()
	{
		return this.databaseCluster.getName();
	}

	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#isActive(java.lang.String)
	 */
	public boolean isActive(String databaseId)
	{
		return this.databaseCluster.isActive(databaseId);
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
