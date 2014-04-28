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
package net.sf.hajdbc.sql;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import net.sf.hajdbc.Credentials;
import net.sf.hajdbc.Locality;
import net.sf.hajdbc.codec.Decoder;
import net.sf.hajdbc.management.Description;
import net.sf.hajdbc.management.MBean;

/**
 * @author  Paul Ferraro
 */
@MBean
@Description("Database accessed via Driver")
public class DriverDatabase extends AbstractDatabase<Driver>
{
	public static String parseVendor(String url)
	{
		return url.substring(5, url.indexOf(":", 5));
	}
	
	private final String url;
	private final Properties properties;

	public DriverDatabase(String id, Driver driver, String url, Properties properties, Credentials credentials, int weight, Locality locality)
	{
		super(id, driver, credentials, weight, locality);
		this.url = url;
		this.properties = properties;
	}

	@Override
	public String getLocation()
	{
		return this.url;
	}

	@Override
	public Properties getProperties()
	{
		return this.properties;
	}

	@Override
	public Connection connect(Decoder decoder) throws SQLException
	{
		Properties properties = this.properties;
		Credentials credentials = this.getCredentials();
		if (credentials != null)
		{
			properties = new Properties(this.properties);
			properties.setProperty("user", credentials.getUser());
			properties.setProperty("password", credentials.decodePassword(decoder));
		}
		return this.getConnectionSource().connect(this.url, properties);
	}
}
