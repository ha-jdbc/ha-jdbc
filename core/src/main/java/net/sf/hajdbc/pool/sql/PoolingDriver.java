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
package net.sf.hajdbc.pool.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.regex.Pattern;

import net.sf.hajdbc.AbstractDriver;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggerFactory;

/**
 * @author Paul Ferraro
 */
public class PoolingDriver extends AbstractDriver
{
	private static final Pattern URL_PATTERN = Pattern.compile("jdbc:ha-jdbc:pool:(.+)"); //$NON-NLS-1$

	private static final Logger logger = LoggerFactory.getLogger(PoolingDriver.class);
	
	static
	{
		try
		{
			DriverManager.registerDriver(new PoolingDriver());
		}
		catch (SQLException e)
		{
			logger.log(Level.ERROR, e, Messages.DRIVER_REGISTER_FAILED.getMessage(), PoolingDriver.class.getName());
		}
	}

	/**
	 * {@inheritDoc}
	 * @see java.sql.Driver#connect(java.lang.String, java.util.Properties)
	 */
	@Override
	public Connection connect(String arg0, Properties arg1)
	{
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see java.sql.Driver#getPropertyInfo(java.lang.String, java.util.Properties)
	 */
	@Override
	public DriverPropertyInfo[] getPropertyInfo(String arg0, Properties arg1)
	{
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.AbstractDriver#getUrlPattern()
	 */
	@Override
	protected Pattern getUrlPattern()
	{
		return URL_PATTERN;
	}

	@Override
	public java.util.logging.Logger getParentLogger()
	{
		// TODO Auto-generated method stub
		return null;
	}
}
