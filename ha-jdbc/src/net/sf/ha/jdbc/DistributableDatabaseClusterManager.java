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
public class DistributableDatabaseClusterManager extends LocalDatabaseClusterManager implements NotificationBus.Consumer
{
	private static final String BUS_NAME = "DatabaseClusterManager";
	private static Log log = LogFactory.getLog(DistributableDatabaseClusterManager.class);
	
	private NotificationBus notificationBus;
	
	public void setProtocol(String protocol) throws Exception
	{
		this.notificationBus = new NotificationBus(BUS_NAME, protocol);
		this.notificationBus.setConsumer(this);
		this.notificationBus.start();
	}
	
	protected void finalize()
	{
		if (this.notificationBus != null)
		{
			this.notificationBus.stop();
		}
	}
	
	public void deactivate(DatabaseCluster databaseCluster, Database database)
	{
		String clusterName = databaseCluster.getDescriptor().getName();
		
		super.deactivate(clusterName, database);
		
		this.notificationBus.sendNotification(new String[] { clusterName, database.getId() });
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
