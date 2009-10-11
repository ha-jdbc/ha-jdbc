/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2009 Paul Ferraro
 * 
 * This library is free software; you can redistribute it and/or modify it 
 * under the terms of the GNU Lesser General Public License as published by the 
 * Free Software Foundation; either version 2.1 of the License, or (at your 
 * option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License 
 * for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation, 
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 * 
 * Contact: ferraro@users.sourceforge.net
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
				provider.getLogger(LoggerFactory.class).log(Level.INFO, "Using {0}", provider.getClass().getName());
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
