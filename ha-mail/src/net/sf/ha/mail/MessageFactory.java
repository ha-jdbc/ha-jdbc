package net.sf.ha.mail;

import javax.mail.Message;
import javax.mail.Session;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public interface MessageFactory
{
	public Message createMessage(Session session);
}
