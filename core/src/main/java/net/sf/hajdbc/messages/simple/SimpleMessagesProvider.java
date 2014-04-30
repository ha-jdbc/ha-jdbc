package net.sf.hajdbc.messages.simple;

import net.sf.hajdbc.messages.Messages;
import net.sf.hajdbc.messages.MessagesProvider;

public class SimpleMessagesProvider implements MessagesProvider
{
	@Override
	public boolean isEnabled()
	{
		return true;
	}

	@Override
	public Messages getMessages()
	{
		return new SimpleMessages();
	}

	@Override
	public String getName()
	{
		return "simple";
	}
}
