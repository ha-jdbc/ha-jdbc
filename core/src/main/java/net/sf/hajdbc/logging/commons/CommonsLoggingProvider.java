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
package net.sf.hajdbc.logging.commons;

import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggingProvider;

/**
 * <a href="Apache Commons">http://commons.apache.org/logging</a> logging service provider.
 * 
 * @author Paul Ferraro
 */
public class CommonsLoggingProvider implements LoggingProvider
{
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.logging.LoggingProvider#getLogger(java.lang.Class)
	 */
	@Override
	public Logger getLogger(Class<?> targetClass)
	{
		return new CommonsLogger(targetClass);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.logging.LoggingProvider#getName()
	 */
	@Override
	public String getName()
	{
		return "Commons";
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.logging.LoggingProvider#isEnabled()
	 */
	@Override
	public boolean isEnabled()
	{
		try
		{
			org.apache.commons.logging.LogFactory.getFactory();
			
			return true;
		}
		catch (Throwable e)
		{
			return false;
		}
	}
}
