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
import java.util.Collection;
import java.util.concurrent.locks.Lock;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseClusterFactory;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.SQLException;
import net.sf.hajdbc.local.LocalDatabaseCluster;

import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.JChannel;
import org.jgroups.blocks.NotificationBus;
import org.jgroups.jmx.JmxConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decorates an existing database cluster by providing distributable functionality.
 * Broadcasts database activations and deactivations to other cluster instances on the network.
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class DistributableDatabaseCluster extends LocalDatabaseCluster implements NotificationBus.Consumer
{
	static Logger logger = LoggerFactory.getLogger(DistributableDatabaseCluster.class);
	
	private NotificationBus notificationBus;
	private DistributableLock lock;
	private DistributableDatabaseClusterBuilder builder;
	
	/**
	 * Constructs a new DistributableDatabaseCluster.
	 * @param builder a builder for this database cluster
	 */
	public DistributableDatabaseCluster(DistributableDatabaseClusterBuilder builder)
	{
		this.builder = builder;
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#deactivate(net.sf.hajdbc.Database)
	 */
	@Override
	public synchronized boolean deactivate(Database database)
	{
		boolean deactivated = super.deactivate(database);
		
		if (deactivated)
		{
			this.notificationBus.sendNotification(new DatabaseDeactivationCommand(database));
		}
		
		return deactivated;
	}

	public boolean deactivateFromCommand(String databaseId)
	{
		return super.deactivate(this.getDatabase(databaseId));
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#activate(net.sf.hajdbc.Database)
	 */
	@Override
	public synchronized boolean activate(Database database)
	{
		boolean activated = super.activate(database);
		
		if (activated)
		{
			this.notificationBus.sendNotification(new DatabaseActivationCommand(database));
		}
		
		return activated;
	}

	public boolean activateFromCommand(String databaseId)
	{
		return super.activate(this.getDatabase(databaseId));
	}

	/**
	 * @see org.jgroups.blocks.NotificationBus.Consumer#handleNotification(java.io.Serializable)
	 */
	public void handleNotification(Serializable command)
	{
		logger.info(Messages.getMessage(Messages.DATABASE_COMMAND_RECEIVED, command));
		
		DatabaseCommand.class.cast(command).execute(this);
	}

	/**
	 * @see org.jgroups.blocks.NotificationBus.Consumer#getCache()
	 */
	public Serializable getCache()
	{
		Collection<String> databases = super.getActiveDatabases();
		
		return databases.toArray(new String[databases.size()]);
	}

	/**
	 * @see org.jgroups.blocks.NotificationBus.Consumer#memberJoined(org.jgroups.Address)
	 */
	public void memberJoined(Address address)
	{
		String channel = this.notificationBus.getChannel().getChannelName();
		
		logger.info(Messages.getMessage(Messages.GROUP_MEMBER_JOINED, address, channel));
	}

	/**
	 * @see org.jgroups.blocks.NotificationBus.Consumer#memberLeft(org.jgroups.Address)
	 */
	public void memberLeft(Address address)
	{
		String channel = this.notificationBus.getChannel().getChannelName();
		
		logger.info(Messages.getMessage(Messages.GROUP_MEMBER_LEFT, address, channel));
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#loadState()
	 */
	@Override
	public String[] loadState() throws java.sql.SQLException
	{
		String[] state = String[].class.cast(this.notificationBus.getCacheFromCoordinator(this.builder.getTimeout(), 1));
		
		return (state != null) ? state : super.loadState();
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#start()
	 */
	@Override
	public synchronized void start() throws java.sql.SQLException
	{
		try
		{
			this.notificationBus = new NotificationBus(this.getId(), this.builder.getProtocol());
			this.notificationBus.setConsumer(this);
			this.notificationBus.start();
			
			this.register(this.notificationBus.getChannel());
			
			this.lock = new DistributableLock(this.getId() + "-lock", this.builder.getProtocol(), this.builder.getTimeout(), super.writeLock());
			
			this.register(this.lock.getChannel());

			super.start();
		}
		catch (Exception e)
		{
			throw new SQLException(e.toString(), e);
		}
	}

	private void register(Channel channel) throws Exception
	{
		MBeanServer server = DatabaseClusterFactory.getMBeanServer();

		ObjectName name = this.getObjectName(channel);
		
		if (!server.isRegistered(name))
		{
			JmxConfigurator.registerChannel(JChannel.class.cast(channel), server, name.getCanonicalName(), true);
		}
	}
	
	private ObjectName getObjectName(Channel channel) throws Exception
	{
		return ObjectName.getInstance("org.jgroups", "channel", ObjectName.quote(channel.getChannelName()));
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#stop()
	 */
	@Override
	public synchronized void stop()
	{
		if (this.notificationBus != null)
		{
			this.unregister(this.notificationBus.getChannel());
			
			this.notificationBus.stop();
		}

		if (this.lock != null)
		{
			this.unregister(this.lock.getChannel());
			
			this.lock.stop();
		}
		
		super.stop();
	}
	
	private void unregister(Channel channel)
	{
		MBeanServer server = DatabaseClusterFactory.getMBeanServer();
		
		try
		{	
			ObjectName name = this.getObjectName(channel);
			
			if (server.isRegistered(name))
			{
				JmxConfigurator.unregisterChannel(server, name);
				JmxConfigurator.unregisterProtocols(server, JChannel.class.cast(channel), name.getCanonicalName());
			}
		}
		catch (Exception e)
		{
			logger.warn(e.getMessage(), e);
		}
	}
	
	/**
	 * @see net.sf.hajdbc.local.LocalDatabaseCluster#writeLock()
	 */
	@Override
	public Lock writeLock()
	{
		return this.lock;
	}
}
