package net.sf.ha.mail;

import javax.mail.Address;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.event.TransportListener;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public interface MailSender
{
	/**
	 * Sends the specified message to the specified addresses using notifying the specified listener of the outcome.
	 * @param message a JavaMail message
	 * @param addresses addresses to which to send this message
	 * @param listener object to be notified of sending outcome.
	 * @throws MessagingException if message cannot be sent.
	 */
	public void send(Message message, Address[] addresses, TransportListener listener) throws MessagingException;
}
