package net.sf.ha.mail;

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
		sender.send(sender.nextAvailableTransport(), message, addresses);
	}
}
