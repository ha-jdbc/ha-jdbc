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
package net.sf.hajdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Paul Ferraro
 * @version $Revision$
 */
public final class Driver implements java.sql.Driver
{
	private static final int MAJOR_VERSION = 1;
	private static final int MINOR_VERSION = 0;
	private static final boolean JDBC_COMPLIANT = true;

	private static Log log = LogFactory.getLog(Driver.class);
	
	static
	{
		try
		{
			DriverManager.registerDriver(new Driver());
		}
		catch (SQLException e)
		{
			log.fatal("Failed to register " + Driver.class.getName(), e);
		}
	}
	
	private Driver()
	{
		// private
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
	
	/**
	 * @see java.sql.Driver#acceptsURL(java.lang.String)
	 */
	public boolean acceptsURL(String url) throws SQLException
	{
		return url.startsWith("jdbc:ha-jdbc:") && (DatabaseClusterFactory.getInstance().getDatabaseCluster(url) != null);
	}
	
	/**
	 * @see java.sql.Driver#connect(java.lang.String, java.util.Properties)
	 */
	public Connection connect(String url, final Properties properties) throws SQLException
	{
		DatabaseCluster databaseCluster = DatabaseClusterFactory.getInstance().getDatabaseCluster(url);
		
		if (databaseCluster == null)
		{
			return null;
		}
		
		ConnectionFactoryProxy connectionFactory = databaseCluster.getConnectionFactory();
		
		DriverOperation operation = new DriverOperation()
		{
			public Object execute(DriverDatabase database, java.sql.Driver driver) throws SQLException
			{
				return driver.connect(database.getUrl(), properties);
			}
		};
		
		return new ConnectionProxy(connectionFactory, connectionFactory.executeWrite(operation));
	}
	
	/**
	 * @see java.sql.Driver#getPropertyInfo(java.lang.String, java.util.Properties)
	 */
	public DriverPropertyInfo[] getPropertyInfo(String url, final Properties properties) throws SQLException
	{
		DatabaseCluster databaseCluster = DatabaseClusterFactory.getInstance().getDatabaseCluster(url);
		
		if (databaseCluster == null)
		{
			return null;
		}
		
		ConnectionFactoryProxy connectionFactory = databaseCluster.getConnectionFactory();
		
		DriverOperation operation = new DriverOperation()
		{
			public Object execute(DriverDatabase database, java.sql.Driver driver) throws SQLException
			{
				return driver.getPropertyInfo(database.getUrl(), properties);
			}
		};
		
		return (DriverPropertyInfo[]) connectionFactory.executeGet(operation);
	}
}
