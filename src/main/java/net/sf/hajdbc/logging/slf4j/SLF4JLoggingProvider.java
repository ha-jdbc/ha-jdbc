/*
 * HA-JDBC: High-Availability JDBC
 * Copyright 2004, 2009 Paul Ferraro
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
package net.sf.hajdbc.logging.slf4j;

import org.slf4j.LoggerFactory;

/**
 * @author paul
 *
 */
public class SLF4JLoggingProvider implements net.sf.hajdbc.logging.LoggingProvider
{
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.logging.LoggerFactory#createLogger(java.lang.Class)
	 */
	@Override
	public net.sf.hajdbc.logging.Logger getLogger(Class<?> targetClass)
	{
		return new SLF4JLogger(targetClass);
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
			LoggerFactory.getILoggerFactory();
			
			return true;
		}
		catch (Throwable e)
		{
			return false;
		}
	}
}
