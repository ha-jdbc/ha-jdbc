package net.sf.hajdbc.messages;

public interface MessagesProvider
{
	boolean isEnabled();

	Messages getMessages();
	
	String getName();
}
