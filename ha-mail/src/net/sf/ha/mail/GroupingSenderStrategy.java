package net.sf.ha.mail;

import java.util.Collection;
import java.util.Iterator;

import javax.mail.Address;
import javax.mail.Message;
import javax.mail.MessagingException;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public abstract class GroupingSenderStrategy extends SimpleSenderStrategy
{
	/**
	 * @see net.sf.ha.mail.SenderStrategy#send(net.sf.ha.mail.Sender, javax.mail.Message, javax.mail.Address[])
	 */
	public void send(Sender sender, Message message, Address[] addresses) throws MessagingException
	{
		if (addresses.length > 1)
		{
			Iterator addressGroups = this.groupAddresses(addresses).iterator();
			
			while (addressGroups.hasNext())
			{
				Address[] addressGroup = (Address[]) addressGroups.next();
				
				sender.send(sender.nextAvailableTransport(), message, addressGroup);
			}
		}
		else
		{
			sender.send(sender.nextAvailableTransport(), message, addresses);
		}
	}
	
	/**
	 * Organizes the specified addresses into groups.
	 * @param addresses all recipients of the message to be sent
	 * @return a collection of javax.mail.Address[]
	 */
	protected abstract Collection groupAddresses(Address[] addresses);
}
