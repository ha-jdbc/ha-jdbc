package net.sf.ha.mail;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.event.TransportListener;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public interface MailSenderStrategy
{
	/**
	 * Send the specified message using the specified sender notifying the specified listener of the outcome.
	 * @param sender object responsible for actually sending the message.
	 * @param message JavaMail message to send
	 * @param listener object to be notified of sending outcome
	 * @throws MessagingException if message cannot be sent
	 */
	public void send(MailSender sender, Message message, TransportListener listener) throws MessagingException;
}
