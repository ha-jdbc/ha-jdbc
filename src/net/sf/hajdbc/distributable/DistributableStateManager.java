/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2006 Paul Ferraro
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
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.StateManager;

import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.blocks.NotificationBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Paul Ferraro
 *
 */
public class DistributableStateManager implements StateManager, NotificationBus.Consumer
{
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	
	private Channel channel;
	private int timeout;
	private NotificationBus notificationBus;
	private DatabaseCluster<?> databaseCluster;
	private StateManager stateManager;
	
	public DistributableStateManager(DatabaseCluster<?> databaseCluster, DistributableDatabaseClusterDecorator decorator) throws Exception
	{
		this.databaseCluster = databaseCluster;
		this.channel = decorator.createChannel(databaseCluster.getId());
		
		this.notificationBus = new NotificationBus(this.channel, this.databaseCluster.getId());
		this.notificationBus.setConsumer(this);
		
		this.timeout = decorator.getTimeout();
		this.stateManager = databaseCluster.getStateManager();
	}

	/**
	 * @see net.sf.hajdbc.StateManager#getInitialState()
	 */
	public Set<String> getInitialState()
	{
		String[] state = String[].class.cast(this.notificationBus.getCacheFromCoordinator(this.timeout, 1));
		
		return (state != null) ? new TreeSet<String>(Arrays.asList(state)) : this.stateManager.getInitialState();
	}
	
	/**
	 * @see net.sf.hajdbc.StateManager#add(java.lang.String)
	 */
	public void add(String databaseId)
	{
		this.notificationBus.sendNotification(new DatabaseActivationCommand(databaseId));
		
		this.stateManager.add(databaseId);
	}

	/**
	 * @see net.sf.hajdbc.StateManager#remove(java.lang.String)
	 */
	public void remove(String databaseId)
	{
		this.notificationBus.sendNotification(new DatabaseDeactivationCommand(databaseId));
		
		this.stateManager.remove(databaseId);
	}

	/**
	 * @see org.jgroups.blocks.NotificationBus.Consumer#getCache()
	 */
	public Serializable getCache()
	{
		List<String> list = new LinkedList<String>();
		
		for (Database<?> database: this.databaseCluster.getBalancer().all())
		{
			list.add(database.getId());
		}
		
		return list.toArray(new String[list.size()]);
	}

	/**
	 * @see org.jgroups.blocks.NotificationBus.Consumer#handleNotification(java.io.Serializable)
	 */
	public void handleNotification(Serializable command)
	{
		this.logger.info(Messages.getMessage(Messages.DATABASE_COMMAND_RECEIVED, command));
		
		DatabaseCommand.class.cast(command).execute(this.databaseCluster);
	}

	/**
	 * @see org.jgroups.blocks.NotificationBus.Consumer#memberJoined(org.jgroups.Address)
	 */
	public void memberJoined(Address address)
	{
		this.logger.info(Messages.getMessage(Messages.GROUP_MEMBER_JOINED, address, this.databaseCluster));
	}

	/**
	 * @see org.jgroups.blocks.NotificationBus.Consumer#memberLeft(org.jgroups.Address)
	 */
	public void memberLeft(Address address)
	{
		this.logger.info(Messages.getMessage(Messages.GROUP_MEMBER_LEFT, address, this.databaseCluster));
	}

	/**
	 * @see net.sf.hajdbc.StateManager#start()
	 */
	public void start() throws Exception
	{
		this.channel.connect(this.channel.getClusterName());
		
		this.stateManager.start();
	}

	/**
	 * @see net.sf.hajdbc.StateManager#stop()
	 */
	public void stop()
	{
		this.channel.close();
		
		this.stateManager.stop();
	}
}
