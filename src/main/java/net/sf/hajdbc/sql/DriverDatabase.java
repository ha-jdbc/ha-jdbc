/*
 * HA-JDBC: High-Availability JDBC
 * Copyright 2004-2009 Paul Ferraro
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
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import net.sf.hajdbc.Messages;
import net.sf.hajdbc.management.Description;
import net.sf.hajdbc.management.MBean;
import net.sf.hajdbc.management.ManagedAttribute;

/**
 * @author  Paul Ferraro
 * @version $Revision: 1948 $
 * @since   1.0
 */
@MBean
@Description("Database accessed via DriverManager")
@XmlType(name = "database")
@XmlAccessorType(XmlAccessType.FIELD)
public class DriverDatabase extends AbstractDatabase<Driver>
{
	@XmlTransient
	private static final String USER = "user"; //$NON-NLS-1$
	@XmlTransient
	private static final String PASSWORD = "password"; //$NON-NLS-1$

	/**
	 * Set the url for this database
	 * @param url a database url
	 * @throws IllegalArgumentException if url is not accepted by any driver
	 */
	@ManagedAttribute
	@Description("JDBC url")
	@Override
	public void setName(String location)
	{
		this.getDriver(location);

		super.setName(location);
	}
	
	/**
	 * @see net.sf.hajdbc.Database#connect(java.lang.Object)
	 */
	@Override
	public Connection connect(Driver driver, String password) throws SQLException
	{
		Properties properties = new Properties();
		
		for (Map.Entry<String, String> entry: this.getProperties().entrySet())
		{
			properties.setProperty(entry.getKey(), entry.getValue());
		}
		
		if (this.requiresAuthentication())
		{
			properties.setProperty(USER, this.getUser());
			properties.setProperty(PASSWORD, password);
		}
		
		return driver.connect(this.getName(), properties);
	}

	/**
	 * @see net.sf.hajdbc.Database#createConnectionSource()
	 */
	@Override
	public Driver createConnectionSource()
	{
		return this.getDriver(this.getName());
	}

	private Driver getDriver(String url)
	{
		try
		{
			return DriverManager.getDriver(url);
		}
		catch (SQLException e)
		{
			throw new IllegalArgumentException(Messages.JDBC_URL_REJECTED.getMessage(url), e);
		}
	}
}
