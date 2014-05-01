package net.sf.hajdbc.messages.i18n;

import java.util.ResourceBundle;

import net.sf.hajdbc.messages.Messages;
import net.sf.hajdbc.messages.MessagesProvider;

public class I18nMessagesProvider implements MessagesProvider
{
	@Override
	public boolean isEnabled()
	{
		try
		{
			// Make sure gettext-commons is on classpath
			this.getClass().getClassLoader().loadClass("org.xnap.commons.i18n.I18nFactory");
			// Ensure requisite resource bundle exists
			ResourceBundle.getBundle(net.sf.hajdbc.messages.i18n.I18nMessages.class.getName());
			return true;
		}
		catch (Throwable e)
		{
			return false;
		}
	}

	@Override
	public Messages getMessages()
	{
		return new I18nMessages(net.sf.hajdbc.messages.i18n.I18nMessages.class.getName());
	}

	@Override
	public String getName()
	{
		return "gettext";
	}
}
