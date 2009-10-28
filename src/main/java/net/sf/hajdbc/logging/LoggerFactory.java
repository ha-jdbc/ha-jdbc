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
package net.sf.hajdbc.logging;

import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;



/**
 * @author paul
 */
public final class LoggerFactory
{
	private static final LoggingProvider provider = getProvider();
	
	private static LoggingProvider getProvider()
	{
		for (LoggingProvider provider: ServiceLoader.load(LoggingProvider.class))
		{
			if (provider.isEnabled())
			{
				provider.getLogger(LoggerFactory.class).log(Level.DEBUG, "Using {0} logging", provider.getName());
				
				return provider;
			}
		}
		
		throw new ServiceConfigurationError(LoggingProvider.class.getName());
	}
	
	public static Logger getLogger(Class<?> targetClass)
	{
		return provider.getLogger(targetClass);
	}
	
	private LoggerFactory()
	{
		// Hide
	}
}
