package net.sf.hajdbc.util;

import java.security.AccessController;
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
		return AccessController.doPrivileged(action);
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
		return AccessController.doPrivileged(action);
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
		AccessController.doPrivileged(action);
	}
	
	private SystemProperties()
	{
		// Hide
	}
}
