package net.sf.ha.mail;

import javax.mail.Address;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Transport;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public interface Sender
{
	/**
	 * Sends the specified message to the specified addresses.
	 * @param message a JavaMail message
	 * @param addresses addresses to which to send this message
	 * @throws MessagingException if message cannot be sent.
	 */
	public void send(Transport transport, Message message, Address[] addresses) throws MessagingException;
	
	public Transport nextAvailableTransport();
}
