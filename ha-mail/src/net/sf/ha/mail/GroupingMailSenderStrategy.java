package net.sf.ha.mail;

import java.util.Collection;
import java.util.Iterator;

import javax.mail.Address;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.event.TransportListener;

/**
 * Abstract sender implementation that sends the message to groups of recipients rather than all at once.
 * 
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
	
	/**
	 * Organizes the specified addresses into groups.
	 * @param addresses all recipients of the message to be sent
	 * @return a collection of javax.mail.Address[]
	 */
	public abstract Collection groupAddresses(Address[] addresses);
}
