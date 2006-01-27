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
import java.util.Map;
import java.util.concurrent.ExecutorService;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;

import net.sf.hajdbc.AbstractDatabaseCluster;
import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.SynchronizationStrategy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.ChannelException;
import org.jgroups.JChannel;
import org.jgroups.blocks.LockManager;
import org.jgroups.blocks.LockNotGrantedException;
import org.jgroups.blocks.LockNotReleasedException;
import org.jgroups.blocks.NotificationBus;
import org.jgroups.blocks.TwoPhaseVotingAdapter;
import org.jgroups.blocks.VotingAdapter;
import org.jgroups.jmx.JmxConfigurator;

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
	
	private DatabaseCluster databaseCluster;
	private NotificationBus notificationBus;
	private LockManager lockManager;
	private int timeout;
	
	/**
	 * Constructs a new DistributableDatabaseCluster.
	 * @param databaseCluster a database cluster to decorate
	 * @param decorator a decorator for this database cluster
	 * @throws Exception if database cluster could not be decorated
	 */
	public DistributableDatabaseCluster(DatabaseCluster databaseCluster, DistributableDatabaseClusterDecorator decorator) throws Exception
	{
		this.databaseCluster = databaseCluster;
		this.timeout = decorator.getTimeout();
		
		this.notificationBus = new NotificationBus(databaseCluster.getId(), decorator.getProtocol());
		this.notificationBus.setConsumer(this);
		this.notificationBus.start();
		
		this.lockManager = new org.jgroups.blocks.DistributedLockManager(new TwoPhaseVotingAdapter(new VotingAdapter(this.notificationBus.getChannel())), databaseCluster.getId());
		
		MBeanServer server = MBeanServer.class.cast(MBeanServerFactory.findMBeanServer(null).get(0));
		
		ObjectName name = ObjectName.getInstance("org.jgroups", "channel", ObjectName.quote(databaseCluster.getId()));
		
		if (!server.isRegistered(name))
		{
			JmxConfigurator.registerChannel(JChannel.class.cast(this.notificationBus.getChannel()), server, name.getCanonicalName(), true);
		}
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
	public void handleNotification(Serializable command)
	{
		log.info(Messages.getMessage(Messages.DATABASE_COMMAND_RECEIVED, command.getClass().getName()));
		
		try
		{
			DatabaseCommand.class.cast(command).execute(this.databaseCluster);
		}
		catch (java.sql.SQLException e)
		{
			log.error(Messages.getMessage(Messages.DATABASE_COMMAND_FAILED, command, this.databaseCluster), e);
		}
	}

	/**
	 * @see org.jgroups.blocks.NotificationBus.Consumer#getCache()
	 */
	public Serializable getCache()
	{
		Collection<String> databases = this.databaseCluster.getActiveDatabases();
		
		return databases.toArray(new String[databases.size()]);
	}

	/**
	 * @see org.jgroups.blocks.NotificationBus.Consumer#memberJoined(org.jgroups.Address)
	 */
	public void memberJoined(Address address)
	{
		String channel = this.notificationBus.getChannel().getChannelName();
		
		log.info(Messages.getMessage(Messages.GROUP_MEMBER_JOINED, address, channel));
	}

	/**
	 * @see org.jgroups.blocks.NotificationBus.Consumer#memberLeft(org.jgroups.Address)
	 */
	public void memberLeft(Address address)
	{
		String channel = this.notificationBus.getChannel().getChannelName();
		
		log.info(Messages.getMessage(Messages.GROUP_MEMBER_LEFT, address, channel));
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
	 * @see net.sf.hajdbc.DatabaseCluster#loadState()
	 */
	public String[] loadState() throws java.sql.SQLException
	{
		String[] state = String[].class.cast(this.notificationBus.getCacheFromCoordinator(this.timeout, 1));
		
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
	public Database getDatabase(String databaseId)
	{
		return this.databaseCluster.getDatabase(databaseId);
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#getActiveDatabases()
	 */
	public Collection<String> getActiveDatabases()
	{
		return this.databaseCluster.getActiveDatabases();
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#getInactiveDatabases()
	 */
	public Collection<String> getInactiveDatabases()
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
	 * @see net.sf.hajdbc.DatabaseCluster#getConnectionFactoryMap()
	 */
	public Map<Database, ?> getConnectionFactoryMap()
	{
		return this.databaseCluster.getConnectionFactoryMap();
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getDefaultSynchronizationStrategy()
	 */
	public SynchronizationStrategy getDefaultSynchronizationStrategy()
	{
		return this.databaseCluster.getDefaultSynchronizationStrategy();
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getExecutor()
	 */
	public ExecutorService getExecutor()
	{
		return this.databaseCluster.getExecutor();
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getDialect()
	 */
	public Dialect getDialect()
	{
		return this.databaseCluster.getDialect();
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#lock(Object)
	 */
	public void lock(Object object)
	{
		try
		{
			this.lockManager.lock(object, this.getId(), 0);
			
			this.databaseCluster.lock(object);
		}
		catch (LockNotGrantedException e)
		{
			throw new IllegalStateException(e);
		}
		catch (ClassCastException e)
		{
			throw new IllegalStateException(e);
		}
		catch (ChannelException e)
		{
			throw new IllegalStateException(e);
		}		
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#unlock(Object)
	 */
	public void unlock(Object object)
	{
		try
		{
			this.lockManager.unlock(object, this.getId());
			
			this.databaseCluster.unlock(object);
		}
		catch (ClassCastException e)
		{
			throw new IllegalStateException(e);
		}
		catch (ChannelException e)
		{
			throw new IllegalStateException(e);
		}
		catch (LockNotReleasedException e)
		{
			throw new IllegalStateException(e);
		}		
	}

	/**
	 * @see net.sf.hajdbc.DatabaseCluster#tryLock(Object)
	 */
	public boolean tryLock(Object object)
	{
		try
		{
			this.lockManager.lock(object, this.getId(), this.timeout);
			
			boolean success = this.databaseCluster.tryLock(object);
			
			if (!success)
			{
				this.lockManager.unlock(object, this.getId());
			}
			
			return success;
		}
		catch (LockNotGrantedException e)
		{
			return false;
		}
		catch (ClassCastException e)
		{
			throw new IllegalStateException(e);
		}
		catch (ChannelException e)
		{
			throw new IllegalStateException(e);
		}		
		catch (LockNotReleasedException e)
		{
			throw new IllegalStateException(e);
		}		
	}
}
