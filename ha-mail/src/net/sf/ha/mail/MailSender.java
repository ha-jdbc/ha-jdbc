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
	public void send(Message message, Address[] addresses, TransportListener listener) throws MessagingException;
}
