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
package net.sf.hajdbc.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseClusterFactory;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.util.SQLExceptionFactory;
import net.sf.hajdbc.util.reflect.ProxyFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 */
public final class Driver implements java.sql.Driver
{
	private static final Pattern URL_PATTERN = Pattern.compile("jdbc:ha-jdbc:(.+)");
	
	private static Logger logger = LoggerFactory.getLogger(Driver.class);
	
	static
	{
		try
		{
			DriverManager.registerDriver(new Driver());
		}
		catch (SQLException e)
		{
			logger.error(Messages.getMessage(Messages.DRIVER_REGISTER_FAILED, Driver.class.getName()), e);
		}
	}
	
	/**
	 * @see java.sql.Driver#acceptsURL(java.lang.String)
	 */
	public boolean acceptsURL(String url)
	{
		return (this.parse(url) != null);
	}

	/**
	 * @see java.sql.Driver#connect(java.lang.String, java.util.Properties)
	 */
	public Connection connect(String url, final Properties properties) throws SQLException
	{
		String id = this.parse(url);
		
		if (id == null) return null;
		
		DatabaseCluster<java.sql.Driver> cluster = this.getDatabaseCluster(id);
		
		DriverInvocationHandler handler = new DriverInvocationHandler(cluster);
		
		Driver driver = ProxyFactory.createProxy(Driver.class, handler);
		
		Invoker<java.sql.Driver, java.sql.Driver, Connection> invoker = new Invoker<java.sql.Driver, java.sql.Driver, Connection>()
		{
			public Connection invoke(Database<java.sql.Driver> database, java.sql.Driver driver) throws SQLException
			{
				return driver.connect(DriverDatabase.class.cast(database).getUrl(), properties);
			}
		};
		
		try
		{
			return new ConnectionInvocationStrategy<java.sql.Driver>(driver).invoke(handler, invoker);
		}
		catch (Exception e)
		{
			throw SQLExceptionFactory.createSQLException(e);
		}
	}

	/**
	 * @see java.sql.Driver#getMajorVersion()
	 */
	public int getMajorVersion()
	{
		return Integer.parseInt(DatabaseClusterFactory.getVersion().split(Pattern.quote("."))[0]);
	}
	
	/**
	 * @see java.sql.Driver#getMinorVersion()
	 */
	public int getMinorVersion()
	{
		return Integer.parseInt(DatabaseClusterFactory.getVersion().split(Pattern.quote("."))[1].split("-")[0]);
	}

	/**
	 * @see java.sql.Driver#getPropertyInfo(java.lang.String, java.util.Properties)
	 */
	public DriverPropertyInfo[] getPropertyInfo(String url, final Properties properties) throws SQLException
	{
		String id = this.parse(url);
		
		if (id == null) return null;
		
		DatabaseCluster<java.sql.Driver> cluster = this.getDatabaseCluster(id);
		
		DriverInvocationHandler handler = new DriverInvocationHandler(cluster);
		
		Invoker<java.sql.Driver, java.sql.Driver, DriverPropertyInfo[]> invoker = new Invoker<java.sql.Driver, java.sql.Driver, DriverPropertyInfo[]>()
		{
			public DriverPropertyInfo[] invoke(Database<java.sql.Driver> database, java.sql.Driver driver) throws SQLException
			{
				return driver.getPropertyInfo(DriverDatabase.class.cast(database).getUrl(), properties);
			}			
		};
		
		try
		{
			return new DriverReadInvocationStrategy<java.sql.Driver, java.sql.Driver, DriverPropertyInfo[]>().invoke(handler, invoker);
		}
		catch (Exception e)
		{
			throw SQLExceptionFactory.createSQLException(e);
		}
	}

	/**
	 * @see java.sql.Driver#jdbcCompliant()
	 */
	public boolean jdbcCompliant()
	{
		return true;
	}
	
	private DatabaseCluster<java.sql.Driver> getDatabaseCluster(String id) throws SQLException
	{
		DatabaseCluster<java.sql.Driver> cluster = DatabaseClusterFactory.getInstance().getDriverDatabaseClusterMap().get(id);
		
		if (cluster == null)
		{
			throw new SQLException(Messages.getMessage(Messages.INVALID_DATABASE_CLUSTER, id));
		}
		
		return cluster;
	}
	
	private String parse(String url)
	{
		Matcher matcher = URL_PATTERN.matcher(url);
		
		if (!matcher.matches())
		{
			return null;
		}
		
		return matcher.group(1);
	}
}
