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
import java.util.Collection;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseClusterDecorator;
import net.sf.hajdbc.Messages;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.blocks.NotificationBus;

/**
 * Decorates an existing database cluster by providing distributable functionality.
 * Broadcasts database activations and deactivations to other cluster instances on the network.
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class DistributableDatabaseCluster extends DatabaseClusterDecorator implements NotificationBus.Consumer
{
	private static Log log = LogFactory.getLog(DistributableDatabaseCluster.class);
	
	private NotificationBus notificationBus;
	
	/**
	 * Constructs a new DistributableDatabaseCluster.
	 * @param databaseCluster a database cluster to decorate
	 * @param descriptor a descriptor of this database cluster
	 * @throws Exception if database cluster could not be decorated
	 */
	public DistributableDatabaseCluster(DatabaseCluster databaseCluster, DistributableDatabaseClusterDescriptor descriptor) throws Exception
	{
		super(databaseCluster);
		
		this.notificationBus = new NotificationBus(databaseCluster.getId(), descriptor.getProtocol());
		this.notificationBus.setConsumer(this);
		this.notificationBus.start();
	}

	/**
	 * @see net.sf.hajdbc.DatabaseCluster#deactivate(net.sf.hajdbc.Database)
	 */
	public boolean deactivate(Database database)
	{
		boolean deactivated = this.databaseCluster.deactivate(database);
		
		if (deactivated)
		{
			this.notificationBus.sendNotification(new DatabaseDeactivationCommand(database.getId()));
		}
		
		return deactivated;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseCluster#activate(net.sf.hajdbc.Database)
	 */
	public boolean activate(Database database)
	{
		boolean activated = this.databaseCluster.activate(database);
		
		if (activated)
		{
			this.notificationBus.sendNotification(new DatabaseActivationCommand(database.getId()));
		}
		
		return activated;
	}

	/**
	 * @see org.jgroups.blocks.NotificationBus.Consumer#handleNotification(java.io.Serializable)
	 */
	public void handleNotification(Serializable object)
	{
		DatabaseCommand command = (DatabaseCommand) object;
		
		if (log.isInfoEnabled())
		{
			log.info(Messages.getMessage(Messages.DATABASE_COMMAND_RECEIVED, command.getClass().getName()));
		}
		
		try
		{
			command.execute(this.databaseCluster);
		}
		catch (java.sql.SQLException e)
		{
			log.error(Messages.getMessage(Messages.DATABASE_COMMAND_FAILED, new Object[] { command, this.databaseCluster }), e);
		}
	}

	/**
	 * @see org.jgroups.blocks.NotificationBus.Consumer#getCache()
	 */
	public Serializable getCache()
	{
		Collection databases = this.databaseCluster.getActiveDatabases();
		
		return (String[]) this.databaseCluster.getActiveDatabases().toArray(new String[databases.size()]);
	}

	/**
	 * @see org.jgroups.blocks.NotificationBus.Consumer#memberJoined(org.jgroups.Address)
	 */
	public void memberJoined(Address address)
	{
		String channel = this.notificationBus.getChannel().getChannelName();
		
		log.info(Messages.getMessage(Messages.GROUP_MEMBER_JOINED, new Object[] { address, channel }));
	}

	/**
	 * @see org.jgroups.blocks.NotificationBus.Consumer#memberLeft(org.jgroups.Address)
	 */
	public void memberLeft(Address address)
	{
		String channel = this.notificationBus.getChannel().getChannelName();
		
		log.info(Messages.getMessage(Messages.GROUP_MEMBER_LEFT, new Object[] { address, channel }));
	}
	
	/**
	 * @see java.lang.Object#finalize()
	 */
	protected void finalize() throws Throwable
	{
		this.notificationBus.stop();
		
		super.finalize();
	}

	/**
	 * @see net.sf.hajdbc.DatabaseCluster#init()
	 */
	public void init() throws java.sql.SQLException
	{
		String[] databases = (String[]) this.notificationBus.getCacheFromCoordinator(5000, 1);
		
		if (databases != null)
		{
			for (int i = 0; i < databases.length; ++i)
			{
				this.databaseCluster.activate(databases[i]);
			}
		}
		else
		{
			this.databaseCluster.init();
		}
	}
}
