package net.sf.ha.mail;

import java.io.IOException;

import javax.mail.Address;
import javax.mail.Message;
import javax.mail.MessagingException;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class SimpleSenderStrategy implements SenderStrategy
{
	/**
	 * @see net.sf.ha.mail.SenderStrategy#send(net.sf.ha.mail.Sender, javax.mail.Message, javax.mail.Address[])
	 */
	public void send(Sender sender, Message message, Address[] addresses) throws MessagingException
	{
		this.validate(message);
		
		sender.send(sender.nextAvailableTransport(), message, addresses);
	}
	
	/**
	 * Validates that the specified message contains the essential parts. 
	 * @param message a JavaMail message
	 * @throws MessagingException if message validation fails
	 */
	protected void validate(Message message) throws MessagingException
	{
		if (message.getSubject() == null)
		{
			message.setSubject("");
		}

		Address[] addresses = message.getAllRecipients();
		
		if ((addresses == null) || (addresses.length == 0))
		{
			throw new MessagingException("Message contains no recipients.");
		}
		
		try
		{
			if (message.getContent() == null)
			{
				message.setText("");
			}
		}
		catch (IOException e)
		{
			throw new MessagingException("Failed to get message content", e);
		}
	}
}
