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
	public void send(MailSender sender, Message message, TransportListener listener) throws MessagingException;
}
