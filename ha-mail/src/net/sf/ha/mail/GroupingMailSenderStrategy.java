package net.sf.ha.mail;

import java.util.Collection;
import java.util.Iterator;

import javax.mail.Address;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.event.TransportListener;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public abstract class GroupingMailSenderStrategy extends SimpleMailSenderStrategy
{
	/**
	 * @see net.sf.ha.mail.SimpleMailSenderStrategy#send(net.sf.ha.mail.MailSender, javax.mail.Message, javax.mail.Address[], javax.mail.event.TransportListener)
	 */
	protected void send(MailSender sender, Message message, Address[] addresses, TransportListener listener) throws MessagingException
	{
		if (addresses.length > 1)
		{
			Iterator addressGroups = this.groupAddresses(addresses).iterator();
			
			while (addressGroups.hasNext())
			{
				Address[] addressGroup = (Address[]) addressGroups.next();
				
				sender.send(message, addressGroup, listener);
			}
		}
		else
		{
			sender.send(message, addresses, listener);
		}
	}
	
	public abstract Collection groupAddresses(Address[] addresses);
}
