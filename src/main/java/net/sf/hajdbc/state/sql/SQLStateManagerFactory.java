/*
 * HA-JDBC: High-Availablity JDBC
 * Copyright 2010 Paul Ferraro
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
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ServiceLoader;

import org.apache.commons.pool.impl.GenericObjectPool;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.pool.generic.GenericObjectPoolFactory;
import net.sf.hajdbc.sql.DriverDatabase;
import net.sf.hajdbc.state.StateManager;
import net.sf.hajdbc.state.StateManagerFactory;

/**
 * @author Paul Ferraro
 */
public class SQLStateManagerFactory extends GenericObjectPool.Config implements StateManagerFactory
{
	enum EmbeddedVendor
	{
		H2("jdbc:h2:{0}"),
		HSQLDB("jdbc:hsqldb:{0}"),
		DERBY("jdbc:derby:{0};create=true")
		;
		
		private final String pattern;
		
		EmbeddedVendor(String pattern)
		{
			this.pattern = pattern;
		}
		
		public String getUrlPattern()
		{
			return this.pattern;
		}
	}
	
	private String urlPattern = this.defaultUrlPattern();
	private String user;
	private String password;
	
	private String defaultUrlPattern()
	{
		ServiceLoader<Driver> drivers = ServiceLoader.load(Driver.class);
		
		for (EmbeddedVendor vendor: EmbeddedVendor.values())
		{
			String pattern = vendor.getUrlPattern();
			String url = MessageFormat.format(pattern, "test");
			
			for (Driver driver: drivers)
			{
				try
				{
					if (driver.acceptsURL(url))
					{
						return pattern;
					}
				}
				catch (SQLException e)
				{
					// Ignore
				}
			}
		}
		
		return null;
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
		
		DriverDatabase database = new DriverDatabase();
		database.setName(MessageFormat.format(this.urlPattern, cluster.getId()));
		database.setUser(this.user);
		database.setPassword(this.password);
		
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
