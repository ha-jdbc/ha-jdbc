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
import net.sf.hajdbc.messages.Messages;
import net.sf.hajdbc.messages.MessagesFactory;
import net.sf.hajdbc.pool.generic.GenericObjectPoolConfiguration;
import net.sf.hajdbc.pool.generic.GenericObjectPoolFactory;
import net.sf.hajdbc.sql.DriverDatabase;
import net.sf.hajdbc.sql.DriverDatabaseBuilder;
import net.sf.hajdbc.state.StateManager;
import net.sf.hajdbc.state.StateManagerFactory;
import net.sf.hajdbc.util.Strings;

/**
 * @author Paul Ferraro
 */
public class SQLStateManagerFactory extends GenericObjectPoolConfiguration implements StateManagerFactory
{
	private static final long serialVersionUID = -544548607415128414L;
	
	private static final Messages messages = MessagesFactory.getMessages();
	private static final Logger logger = LoggerFactory.getLogger(SQLStateManagerFactory.class);
	
	enum EmbeddedVendor
	{
		H2("jdbc:h2:{1}/{0}", "sa", ""),
		HSQLDB("jdbc:hsqldb:{1}/{0}", "sa", ""),
		DERBY("jdbc:derby:{1}/{0};create=true", null, null)
		;
		
		final String pattern;
		final String user;
		final String password;
		
		EmbeddedVendor(String pattern, String user, String password)
		{
			this.pattern = pattern;
			this.user = user;
			this.password = password;
		}
	}
	
	private String urlPattern;
	private String user;
	private String password;

	public SQLStateManagerFactory()
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
						this.urlPattern = vendor.pattern;
						this.user = vendor.user;
						this.password = vendor.password;
					}
				}
			}
			catch (SQLException e)
			{
				// Skip vendor
			}
		}
	}
	
	@Override
	public String getId()
	{
		return "sql";
	}

	/**
	 * {@inheritDoc}
	 * @throws SQLException 
	 * @see net.sf.hajdbc.state.StateManagerFactory#createStateManager(net.sf.hajdbc.DatabaseCluster)
	 */
	@Override
	public <Z, D extends Database<Z>> StateManager createStateManager(DatabaseCluster<Z, D> cluster) throws SQLException
	{
		if (this.urlPattern == null)
		{
			throw new IllegalArgumentException(messages.noEmbeddedDriverFound());
		}
		
		String url = MessageFormat.format(this.urlPattern, cluster.getId(), Strings.HA_JDBC_HOME);
		
		DriverDatabaseBuilder builder = new DriverDatabaseBuilder("").url(url);
		if (this.user != null)
		{
			builder.credentials(this.user, this.password);
		}
		DriverDatabase database = builder.build();
		
		logger.log(Level.INFO, messages.clusterStatePersistence(cluster, url));
		
		return new SQLStateManager<>(cluster, database, new GenericObjectPoolFactory(this));
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
