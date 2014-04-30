package net.sf.hajdbc.messages;

import java.util.ServiceLoader;

import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggerFactory;
import net.sf.hajdbc.logging.LoggingProvider;

public class MessagesFactory
{
	private static final Logger logger = LoggerFactory.getLogger(MessagesFactory.class);
	private static final MessagesProvider provider = getProvider();

	private static MessagesProvider getProvider()
	{
		for (MessagesProvider provider: ServiceLoader.load(MessagesProvider.class, MessagesProvider.class.getClassLoader()))
		{
			if (provider.isEnabled())
			{
				logger.log(Level.DEBUG, "Using {0} messages", provider.getName());
				
				return provider;
			}
		}
		throw new IllegalStateException(String.format("No %s found", LoggingProvider.class.getName()));
	}
	
	public static Messages getMessages()
	{
		return provider.getMessages();
	}
	
	private MessagesFactory()
	{
		// Hide
	}
}
