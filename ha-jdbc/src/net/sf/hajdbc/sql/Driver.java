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
package net.sf.hajdbc.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.hajdbc.ConnectionFactoryProxy;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseClusterFactory;
import net.sf.hajdbc.Messages;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Paul Ferraro
 * @version $Revision$
 */
public final class Driver implements java.sql.Driver
{
	private static final Pattern URL_PATTERN = Pattern.compile("^jdbc:ha-jdbc:(.*)$");
	
	private static final int MAJOR_VERSION = 1;
	private static final int MINOR_VERSION = 0;
	private static final boolean JDBC_COMPLIANT = true;

	private static Log log = LogFactory.getLog(Driver.class);
	
	static
	{
		Driver driver = new Driver();
		
		try
		{
			DriverManager.registerDriver(driver);
		}
		catch (SQLException e)
		{
			log.fatal(Messages.getMessage(Messages.DRIVER_REGISTER_FAILED, driver.getClass().getName()), e);
		}
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
	
	private DatabaseCluster getDatabaseCluster(String url) throws SQLException
	{
		Matcher matcher = URL_PATTERN.matcher(url);
		
		if (!matcher.matches())
		{
			return null;
		}
		
		String name = matcher.group(1);
		
		return DatabaseClusterFactory.getInstance().getDatabaseCluster(name);
	}
	
	/**
	 * @see java.sql.Driver#acceptsURL(java.lang.String)
	 */
	public boolean acceptsURL(String url) throws SQLException
	{
		return (this.getDatabaseCluster(url) != null);
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
		
		ConnectionFactoryProxy connectionFactory = databaseCluster.getConnectionFactory();
		
		DriverOperation operation = new DriverOperation()
		{
			public Object execute(DriverDatabase database, java.sql.Driver driver) throws SQLException
			{
				return driver.connect(database.getUrl(), properties);
			}
		};
		
		return new ConnectionProxy(connectionFactory, operation);
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
		
		ConnectionFactoryProxy connectionFactory = databaseCluster.getConnectionFactory();
		
		DriverOperation operation = new DriverOperation()
		{
			public Object execute(DriverDatabase database, java.sql.Driver driver) throws SQLException
			{
				return driver.getPropertyInfo(database.getUrl(), properties);
			}
		};
		
		return (DriverPropertyInfo[]) connectionFactory.executeReadFromDriver(operation);
	}
}
