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
import java.util.Collection;

import net.sf.hajdbc.AbstractDatabaseCluster;
import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.ConnectionFactory;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.SynchronizationStrategy;

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
public class DistributableDatabaseCluster extends AbstractDatabaseCluster implements NotificationBus.Consumer
{
	private static Log log = LogFactory.getLog(DistributableDatabaseCluster.class);
	
	private NotificationBus notificationBus;
	private DatabaseCluster databaseCluster;
	
	/**
	 * Constructs a new DistributableDatabaseCluster.
	 * @param databaseCluster a database cluster to decorate
	 * @param decorator a decorator for this database cluster
	 * @throws Exception if database cluster could not be decorated
	 */
	public DistributableDatabaseCluster(DatabaseCluster databaseCluster, DistributableDatabaseClusterDecorator decorator) throws Exception
	{
		this.databaseCluster = databaseCluster;
		this.notificationBus = new NotificationBus(databaseCluster.getId(), decorator.getProtocol());
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
			this.notificationBus.sendNotification(new DatabaseDeactivationCommand(database));
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
			this.notificationBus.sendNotification(new DatabaseActivationCommand(database));
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
		catch (SQLException e)
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
	 * @throws SQLException 
	 * @see net.sf.hajdbc.DatabaseCluster#loadState()
	 */
	public String[] loadState() throws SQLException
	{
		String[] state = (String[]) this.notificationBus.getCacheFromCoordinator(1000, 1);
		
		return (state != null) ? state : this.databaseCluster.loadState();
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#getId()
	 */
	public String getId()
	{
		return this.databaseCluster.getId();
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#isAlive(net.sf.hajdbc.Database)
	 */
	public boolean isAlive(Database database)
	{
		return this.databaseCluster.isAlive(database);
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getDatabase(java.lang.String)
	 */
	public Database getDatabase(String databaseId) throws java.sql.SQLException
	{
		return this.databaseCluster.getDatabase(databaseId);
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#getActiveDatabases()
	 */
	public Collection getActiveDatabases()
	{
		return this.databaseCluster.getActiveDatabases();
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#getInactiveDatabases()
	 */
	public Collection getInactiveDatabases()
	{
		return this.databaseCluster.getInactiveDatabases();
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getBalancer()
	 */
	public Balancer getBalancer()
	{
		return this.databaseCluster.getBalancer();
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getConnectionFactory()
	 */
	public ConnectionFactory getConnectionFactory()
	{
		return this.databaseCluster.getConnectionFactory();
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getDefaultSynchronizationStrategy()
	 */
	public SynchronizationStrategy getDefaultSynchronizationStrategy()
	{
		return this.databaseCluster.getDefaultSynchronizationStrategy();
	}
}
