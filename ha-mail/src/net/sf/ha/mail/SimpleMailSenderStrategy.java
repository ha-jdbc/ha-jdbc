package net.sf.ha.mail;

import java.io.IOException;

import javax.mail.Address;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.event.TransportListener;

/**
 * Simple sender implementation that validates the message before sending to the declared recipients of the message.
 * 
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class SimpleMailSenderStrategy implements MailSenderStrategy
{
	/**
	 * @see net.sf.ha.mail.MailSenderStrategy#send(net.sf.ha.mail.MailSender, javax.mail.Message, javax.mail.event.TransportListener)
	 */
	public void send(MailSender sender, Message message, TransportListener listener) throws MessagingException
	{
		this.validate(message);
		
		Address[] addresses = this.getRecipients(message);
		
		this.send(sender, message, addresses, listener);
	}
	
	/**
	 * Extracts the recipients from the specified message
	 * @param message a JavaMail message
	 * @return the message recipients
	 * @throws MessagingException if the message does not contain any recipients
	 */
	protected Address[] getRecipients(Message message) throws MessagingException
	{
		Address[] addresses = message.getAllRecipients();
		
		if ((addresses == null) || (addresses.length == 0))
		{
			throw new MessagingException("Message has no recipients.");
		}
		
		return addresses;
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
	
	/**
	 * Sends the specified message to the specified addresses using the specified sender notifying the specified listener of the outcome.
	 * @param sender responsible for actually sending message
	 * @param message a JavaMail message
	 * @param addresses the message recipients
	 * @param listener object to be notified of sending outcome
	 * @throws MessagingException if send fails
	 */
	protected void send(MailSender sender, Message message, Address[] addresses, TransportListener listener) throws MessagingException
	{
		sender.send(message, addresses, listener);
	}
}
