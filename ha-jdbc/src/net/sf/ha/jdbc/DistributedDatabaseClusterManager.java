package net.sf.ha.jdbc;

import java.io.Serializable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.blocks.NotificationBus;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class DistributedDatabaseClusterManager extends DatabaseClusterManager implements DistributedDatabaseClusterManagerMBean, NotificationBus.Consumer
{
	private static final String BUS_NAME = "DistributedDatabaseClusterManager";
	private static Log log = LogFactory.getLog(DistributedDatabaseClusterManager.class);
	
	private NotificationBus notificationBus;
	
	public DistributedDatabaseClusterManager()
	{
		try
		{
			this.notificationBus = new NotificationBus(BUS_NAME);
			this.notificationBus.start();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	protected void finalize()
	{
		this.notificationBus.stop();
	}
	
	public void deactivate(DatabaseCluster cluster, Database database)
	{
		this.notificationBus.sendNotification(new String[] { cluster.getDescriptor().getName(), database.getId() });
	}

	/**
	 * @see org.jgroups.blocks.NotificationBus.Consumer#handleNotification(java.io.Serializable)
	 */
	public void handleNotification(Serializable object)
	{
		String[] notification = (String[]) object;
		String clusterName = notification[0];
		String databaseId = notification[1];
		
		Database database = (Database) this.getDescriptor(clusterName).getDatabaseMap().get(databaseId);
		
		super.deactivate(clusterName, database);
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
		log.info("Member joined: " + address);
	}

	/**
	 * @see org.jgroups.blocks.NotificationBus.Consumer#memberLeft(org.jgroups.Address)
	 */
	public void memberLeft(Address address)
	{
		log.info("Member left: " + address);
	}
}
