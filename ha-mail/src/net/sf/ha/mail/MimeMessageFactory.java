/*
 * Copyright (c) 2004, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.ha.mail;

import javax.mail.Message;
import javax.mail.Session;
import javax.mail.internet.MimeMessage;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class MimeMessageFactory implements MessageFactory
{
	/**
	 * @see net.sf.ha.mail.MessageFactory#createMessage(javax.mail.Session)
	 */
	public Message createMessage(Session session)
	{
		return new MimeMessage(session);
	}
}
