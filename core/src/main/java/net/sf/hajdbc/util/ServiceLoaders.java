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
import net.sf.hajdbc.messages.Messages;
import net.sf.hajdbc.messages.MessagesFactory;

public class ServiceLoaders
{
	private static final Messages messages = MessagesFactory.getMessages();
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
			throw new IllegalStateException(messages.serviceNotFound(serviceClass));
		}
		return service;
	}

	public static <T> T findService(Class<T> serviceClass, Matcher<T> matcher)
	{
		List<T> matches = new LinkedList<>();
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
			logger.log(Level.WARN, messages.multipleServicesFound(serviceClass, matcher, matches));
		}
		
		return !matches.isEmpty() ? matches.get(0) : null;
	}

	public static <T> T findRequiredService(Class<T> serviceClass, Matcher<T> matcher)
	{
		T service = findService(serviceClass, matcher);
		if (service == null)
		{
			throw new IllegalArgumentException(messages.serviceNotFound(serviceClass, matcher));
		}
		return service;
	}

	private ServiceLoaders()
	{
		// Hide
	}
}
