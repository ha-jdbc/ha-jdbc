package net.sf.hajdbc.util;

import java.util.LinkedList;
import java.util.List;
import java.util.ServiceLoader;

import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggerFactory;

public class ServiceLoaders
{
	private static final Logger logger = LoggerFactory.getLogger(ServiceLoaders.class);
	
	public static <T> T findService(Class<T> serviceClass)
	{
		for (T service: ServiceLoader.load(serviceClass, serviceClass.getClassLoader()))
		{
			return service;
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

		for (T service: ServiceLoader.load(serviceClass, serviceClass.getClassLoader()))
		{
			if (matcher.matches(service))
			{
				matches.add(service);
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
