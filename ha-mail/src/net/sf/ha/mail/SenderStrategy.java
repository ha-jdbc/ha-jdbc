package net.sf.ha.mail;

import javax.mail.Address;
import javax.mail.Message;
import javax.mail.MessagingException;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public interface SenderStrategy
{
	/**
	 * Send the specified message to the specified addresses using the specified sender.
	 * @param sender object responsible for actually sending the message.
	 * @param message JavaMail message to send
	 * @param addresses an array of Addresses to which to sending the message
	 * @throws MessagingException if message cannot be sent
	 */
	public void send(Sender sender, Message message, Address[] address) throws MessagingException;
}
