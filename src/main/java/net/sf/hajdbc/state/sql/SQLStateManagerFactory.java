/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.sf.hajdbc.state.sql;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Collections;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggerFactory;
import net.sf.hajdbc.pool.generic.GenericObjectPoolConfiguration;
import net.sf.hajdbc.pool.generic.GenericObjectPoolFactory;
import net.sf.hajdbc.sql.DriverDatabase;
import net.sf.hajdbc.state.StateManager;
import net.sf.hajdbc.state.StateManagerFactory;
import net.sf.hajdbc.util.Strings;

/**
 * @author Paul Ferraro
 */
public class SQLStateManagerFactory extends GenericObjectPoolConfiguration implements StateManagerFactory
{
	private static final long serialVersionUID = -544548607415128414L;
	
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	
	enum EmbeddedVendor
	{
		H2("jdbc:h2:{1}/{0}"),
		HSQLDB("jdbc:hsqldb:{1}/{0}"),
		DERBY("jdbc:derby:{1}/{0};create=true")
		;
		
		final String pattern;
		
		EmbeddedVendor(String pattern)
		{
			this.pattern = pattern;
		}
	}
	
	private String urlPattern = this.defaultUrlPattern();
	private String user;
	private String password;
	
	private String defaultUrlPattern()
	{
		for (EmbeddedVendor vendor: EmbeddedVendor.values())
		{
			String url = MessageFormat.format(vendor.pattern, "test", Strings.HA_JDBC_HOME);
			
			try
			{
				for (Driver driver: Collections.list(DriverManager.getDrivers()))
				{
					if (driver.acceptsURL(url))
					{
						return vendor.pattern;
					}
				}
			}
			catch (SQLException e)
			{
				// Skip vendor
			}
		}
		
		return null;
	}
	
	@Override
	public String getId()
	{
		return "sql";
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.state.StateManagerFactory#createStateManager(net.sf.hajdbc.DatabaseCluster)
	 */
	@Override
	public <Z, D extends Database<Z>> StateManager createStateManager(DatabaseCluster<Z, D> cluster)
	{
		if (this.urlPattern == null)
		{
			// TODO externalize
			throw new IllegalArgumentException("No urlPattern property defined and no embedded database driver was detected on the classpath.");
		}
		
		String url = MessageFormat.format(this.urlPattern, cluster.getId(), Strings.HA_JDBC_HOME);
		DriverDatabase database = new DriverDatabase();
		database.setLocation(url);
		database.setUser(this.user);
		database.setPassword(this.password);
		
		this.logger.log(Level.INFO, "State for cluster {0} will be persisted to {1}", cluster, url);
		
		return new SQLStateManager<Z, D>(cluster, database, new GenericObjectPoolFactory(this));
	}
	
	public String getUrlPattern()
	{
		return this.urlPattern;
	}
	
	public void setUrlPattern(String urlPattern)
	{
		this.urlPattern = urlPattern;
	}

	public String getUser()
	{
		return this.user;
	}
	
	public void setUser(String user)
	{
		this.user = user;
	}

	public String getPassword()
	{
		return this.password;
	}
	
	public void setPassword(String password)
	{
		this.password = password;
	}
}
