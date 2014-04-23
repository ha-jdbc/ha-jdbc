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

import java.security.PrivilegedAction;
import java.util.Properties;

public class SystemProperties
{
	public static String getSystemProperty(String name)
	{
		return getSystemProperty(name, null);
	}

	public static String getSystemProperty(final String name, final String defaultValue)
	{
		PrivilegedAction<String> action = new PrivilegedAction<String>()
		{
			@Override
			public String run()
			{
				return System.getProperty(name, defaultValue);
			}
		};
		return Security.run(action);
	}

	public static Properties getSystemProperties()
	{
		PrivilegedAction<Properties> action = new PrivilegedAction<Properties>()
		{
			@Override
			public Properties run()
			{
				return System.getProperties();
			}
		};
		return Security.run(action);
	}
	
	public static void setSystemProperty(final String name, final String value)
	{
		PrivilegedAction<Void> action = new PrivilegedAction<Void>()
		{
			@Override
			public Void run()
			{
				System.setProperty(name, value);
				return null;
			}
		};
		Security.run(action);
	}
	
	private SystemProperties()
	{
		// Hide
	}
}
