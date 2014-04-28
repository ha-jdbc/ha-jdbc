/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2014  Paul Ferraro
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
package net.sf.hajdbc.sql;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import net.sf.hajdbc.Locality;

public class DriverDatabaseBuilder extends AbstractDatabaseBuilder<Driver, DriverDatabase>
{
	public DriverDatabaseBuilder(String id)
	{
		super(id);
	}

	/**
	 * Alias for {@link #connectionSource(Driver)}.
	 */
	public DriverDatabaseBuilder driver(Driver driver)
	{
		return this.connectionSource(driver);
	}

	@Override
	public DriverDatabaseBuilder connectionSource(Driver connectionSource)
	{
		super.connectionSource(connectionSource);
		return this;
	}

	/**
	 * Alias for {@link #connectionSource(Driver)}.
	 */
	public DriverDatabaseBuilder url(String url)
	{
		return this.location(url);
	}
	
	@Override
	public DriverDatabaseBuilder location(String location)
	{
		super.location(location);
		return this;
	}

	@Override
	public DriverDatabaseBuilder property(String name, String value)
	{
		super.property(name, value);
		return this;
	}

	@Override
	public DriverDatabaseBuilder credentials(String user, String password)
	{
		super.credentials(user, password);
		return this;
	}

	@Override
	public DriverDatabaseBuilder weight(int weight)
	{
		super.weight(weight);
		return this;
	}

	@Override
	public DriverDatabaseBuilder locality(Locality locality)
	{
		super.locality(locality);
		return this;
	}

	@Override
	public DriverDatabaseBuilder read(DriverDatabase database)
	{
		super.read(database);
		this.location = database.getLocation();
		this.properties = new Properties(database.getProperties());
		return this;
	}

	@Override
	public DriverDatabase build() throws SQLException
	{
		String url = this.location;
		if (url == null)
		{
			throw new SQLException("No location specified");
		}
		
		Driver driver = this.connectionSource;
		if ((driver != null) && !driver.acceptsURL(url))
		{
			throw new SQLException(driver.getClass().getName() + " driver does not accept " + url);
		}
		Driver connectionSource = (driver == null) ? DriverManager.getDriver(url) : driver;
		return new DriverDatabase(this.id, connectionSource, url, new Properties(this.properties), this.credentials, this.weight, this.locality);
	}
}
