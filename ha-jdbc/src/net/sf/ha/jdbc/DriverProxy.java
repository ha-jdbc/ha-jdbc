package net.sf.ha.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Paul Ferraro
 * @version $Revision$
 */
public final class DriverProxy implements java.sql.Driver
{
	private static final int MAJOR_VERSION = 1;
	private static final int MINOR_VERSION = 0;
	private static final boolean JDBC_COMPLIANT = true;

	private static Log log = LogFactory.getLog(DriverProxy.class);
	
	static
	{
		try
		{
			DriverManager.registerDriver(new DriverProxy());
		}
		catch (SQLException e)
		{
			log.fatal("Failed to initialize " + DriverProxy.class.getName(), e);
			throw new RuntimeException(e);
		}
	}

	// Maps cluster name -> DatabaseCluster
	private Map databaseClusterMap;
	
	public DriverProxy() throws SQLException
	{
		DatabaseClusterManager manager = DatabaseClusterManagerFactory.getClusterManager();
		Set clusterSet = manager.getClusterSet(DriverDatabase.class);
		
		Map databaseClusterMap = new HashMap(clusterSet.size());
		
		Iterator clusters = clusterSet.iterator();
		
		while (clusters.hasNext())
		{
			String clusterName = (String) clusters.next();

			DatabaseClusterDescriptor descriptor = manager.getDescriptor(clusterName);
			List databaseList = descriptor.getActiveDatabaseList();
			
			Map driverMap = new HashMap(databaseList.size());
			
			for (int i = 0; i < databaseList.size(); ++i)
			{
				DriverDatabase database = (DriverDatabase) databaseList.get(i);
				
				String driverClassName = database.getDriver();
				
				try
				{
					Class driverClass = Class.forName(driverClassName);
					
					if (!Driver.class.isAssignableFrom(driverClass))
					{
						throw new SQLException(driverClassName + " does not implement " + Driver.class.getName());
					}
					
					String url = database.getUrl();
					Driver driver = DriverManager.getDriver(url);
					
					if (driver == null)
					{
						throw new SQLException(driverClassName + " does not accept url: " + url);
					}
					
					driverMap.put(database, driver);
				}
				catch (ClassNotFoundException e)
				{
					throw new SQLException(driverClassName + " not found in CLASSPATH");
				}
			}
			
			DatabaseCluster databaseCluster = new DatabaseCluster(manager, descriptor, driverMap);
			
			databaseClusterMap.put(clusterName, databaseCluster);
		}
		
		this.databaseClusterMap = Collections.synchronizedMap(databaseClusterMap);
	}
	
	/**
	 * @see java.sql.Driver#getMajorVersion()
	 */
	public int getMajorVersion()
	{
		return MAJOR_VERSION;
	}
	
	/**
	 * @see java.sql.Driver#getMinorVersion()
	 */
	public int getMinorVersion()
	{
		return MINOR_VERSION;
	}
	
	/**
	 * @see java.sql.Driver#jdbcCompliant()
	 */
	public boolean jdbcCompliant()
	{
		return JDBC_COMPLIANT;
	}
	
	private DatabaseCluster getDatabaseCluster(String clusterName)
	{
		return (DatabaseCluster) this.databaseClusterMap.get(clusterName);
	}
	
	/**
	 * @see java.sql.Driver#acceptsURL(java.lang.String)
	 */
	public boolean acceptsURL(String url)
	{
		return this.databaseClusterMap.keySet().contains(url);
	}
	
	/**
	 * @see java.sql.Driver#connect(java.lang.String, java.util.Properties)
	 */
	public Connection connect(String url, final Properties properties) throws SQLException
	{
		DatabaseCluster databaseCluster = this.getDatabaseCluster(url);
		
		if (databaseCluster == null)
		{
			return null;
		}
		
		DriverOperation operation = new DriverOperation()
		{
			public Object execute(DriverDatabase database, Driver driver) throws SQLException
			{
				return driver.connect(database.getUrl(), properties);
			}
		};
		
		return new ConnectionProxy(databaseCluster, databaseCluster.executeWrite(operation));
	}
	
	/**
	 * @see java.sql.Driver#getPropertyInfo(java.lang.String, java.util.Properties)
	 */
	public DriverPropertyInfo[] getPropertyInfo(String url, final Properties properties) throws SQLException
	{
		DatabaseCluster databaseCluster = this.getDatabaseCluster(url);
		
		if (databaseCluster == null)
		{
			return null;
		}
		
		DriverOperation operation = new DriverOperation()
		{
			public Object execute(DriverDatabase database, Driver driver) throws SQLException
			{
				return driver.getPropertyInfo(database.getUrl(), properties);
			}
		};
		
		return (DriverPropertyInfo[]) databaseCluster.executeGet(operation);
	}
}
