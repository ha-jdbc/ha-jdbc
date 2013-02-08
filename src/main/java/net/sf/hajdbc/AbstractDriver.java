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
package net.sf.hajdbc;

import java.sql.Driver;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Paul Ferraro
 */
public abstract class AbstractDriver implements Driver
{
	/**
	 * {@inheritDoc}
	 * @see java.sql.Driver#acceptsURL(java.lang.String)
	 */
	@Override
	public boolean acceptsURL(String url) throws SQLException
	{
		return (this.parse(url) != null);
	}
	
	protected abstract Pattern getUrlPattern();
	
	protected String parse(String url)
	{
		Matcher matcher = this.getUrlPattern().matcher(url);
		
		if (!matcher.matches())
		{
			return null;
		}
		
		return matcher.group(1);
	}

	/**
	 * {@inheritDoc}
	 * @see java.sql.Driver#jdbcCompliant()
	 */
	@Override
	public boolean jdbcCompliant()
	{
		return false;
	}

	/**
	 * @see java.sql.Driver#getMajorVersion()
	 */
	@Override
	public int getMajorVersion()
	{
		return Version.CURRENT.getMajor();
	}
	
	/**
	 * @see java.sql.Driver#getMinorVersion()
	 */
	@Override
	public int getMinorVersion()
	{
		return Version.CURRENT.getMinor();
	}
}
