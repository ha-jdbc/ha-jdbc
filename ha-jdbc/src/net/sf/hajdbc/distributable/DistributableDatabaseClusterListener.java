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

import net.sf.hajdbc.DatabaseClusterListener;
import net.sf.hajdbc.DistributableDatabaseClusterManager;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class DistributableDatabaseClusterListener implements DatabaseClusterListener, NotificationBus.Consumer
{
	private static final String BUS_NAME = "";
	private static Log log = LogFactory.getLog(DistributableDatabaseClusterManager.class);
	
	private NotificationBus notificationBus;
	
	/**
	 * @see org.jgroups.blocks.NotificationBus.Consumer#handleNotification(java.io.Serializable)
	 */
	public void handleNotification(Serializable object)
	{
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
	}

	/**
	 * @see org.jgroups.blocks.NotificationBus.Consumer#memberLeft(org.jgroups.Address)
	 */
	public void memberLeft(Address address)
	{
	}
}
