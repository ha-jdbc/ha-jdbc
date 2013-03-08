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
package net.sf.hajdbc.util;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggerFactory;

public class ServiceLoaders
{
	private static final Logger logger = LoggerFactory.getLogger(ServiceLoaders.class);
	
	public static <T> T findService(Class<T> serviceClass)
	{
		Iterator<T> services = ServiceLoader.load(serviceClass, serviceClass.getClassLoader()).iterator();
		
		while (services.hasNext())
		{
			try
			{
				return services.next();
			}
			catch (ServiceConfigurationError e)
			{
				logger.log(Level.DEBUG, e.getLocalizedMessage());
			}
		}
		return null;
	}

	public static <T> T findRequiredService(Class<T> serviceClass)
	{
		T service = findService(serviceClass);
		if (service == null)
		{
			throw new IllegalStateException(String.format("No %s found", serviceClass.getName()));
		}
		return service;
	}

	public static <T> T findService(Matcher<T> matcher, Class<T> serviceClass)
	{
		List<T> matches = new LinkedList<T>();
		Iterator<T> services = ServiceLoader.load(serviceClass, serviceClass.getClassLoader()).iterator();
		
		while (services.hasNext())
		{
			try
			{
				T service = services.next();
				if (matcher.matches(service))
				{
					matches.add(service);
				}
			}
			catch (ServiceConfigurationError e)
			{
				logger.log(Level.DEBUG, e.getLocalizedMessage());
			}
		}
		
		if (matches.size() > 1)
		{
			logger.log(Level.WARN, "Multiple {0} found matching {1}: {2}", serviceClass.getName(), matcher, matches);
		}
		
		return !matches.isEmpty() ? matches.get(0) : null;
	}

	public static <T> T findRequiredService(Matcher<T> matcher, Class<T> serviceClass)
	{
		T service = findService(matcher, serviceClass);
		if (service == null)
		{
			throw new IllegalArgumentException(String.format("No %s found matching %s", serviceClass.getName(), matcher));
		}
		return service;
	}

	private ServiceLoaders()
	{
		// Hide
	}
}
