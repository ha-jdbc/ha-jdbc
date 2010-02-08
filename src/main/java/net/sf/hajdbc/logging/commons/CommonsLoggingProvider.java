package net.sf.hajdbc.logging.commons;

import org.apache.commons.logging.LogConfigurationException;
import org.apache.commons.logging.LogFactory;

import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggingProvider;

public class CommonsLoggingProvider implements LoggingProvider
{
	@Override
	public Logger getLogger(Class<?> targetClass)
	{
		return new CommonsLogger(targetClass);
	}

	@Override
	public String getName()
	{
		return "Commons";
	}

	@Override
	public boolean isEnabled()
	{
		try
		{
			LogFactory.getFactory();
			
			return true;
		}
		catch (LogConfigurationException e)
		{
			return false;
		}
	}
}
