/*
 * Copyright (c) 2004, Identity Theft 911, LLC.  All rights reserved.
 *
 * $Id$
 */
package net.sf.hajdbc.distributable;

import java.io.Serializable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.blocks.NotificationBus;
import org.jgroups.util.Command;

import net.sf.hajdbc.DatabaseClusterDescriptor;
import net.sf.hajdbc.DatabaseClusterListener;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class DistributableDatabaseClusterListener implements DatabaseClusterListener, NotificationBus.Consumer
{
	private static Log log = LogFactory.getLog(DistributableDatabaseClusterListener.class);
	
	private NotificationBus notificationBus;
	
	public DistributableDatabaseClusterListener(DatabaseClusterDescriptor descriptor, String protocol) throws Exception
	{
		this.notificationBus = new NotificationBus(descriptor.getName(), protocol);
		this.notificationBus.setConsumer(this);
		this.notificationBus.start();
	}
	
	/**
	 * @see org.jgroups.blocks.NotificationBus.Consumer#handleNotification(java.io.Serializable)
	 */
	public void handleNotification(Serializable object)
	{
		Command command = (Command) object;
		
		command.execute();
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
}
