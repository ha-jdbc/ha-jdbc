package net.sf.ha.mail;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import javax.mail.Address;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.SendFailedException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.event.ConnectionAdapter;
import javax.mail.event.ConnectionEvent;
import javax.mail.event.TransportListener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * <p>Utility for efficiently high volume sending of email messages.</p>
 * <ul>
 * 	<li>Messages are sent asynchronously to minimize impact of network latency.</li>
 * 	<li>Maintains a connection pool of transport server connections.</li>
 * 	<li>Ability to configure sending strategies, e.g. email address host grouping.</li>
 * 	<li>Default sender strategy avoids saving messages prior to sending (e.g. Sent Items folder).</li>
 * </ul>
 * <p>Offers high-availibility</p>
 * <ul>
 * 	<li>Transparently manages connections to multiple servers.</li>
 *  <li>Failed sends automatically retry using next available server.</li>
 *  <li>Detected closed connections are automatically re-opened and made available.</li>
 * </ul>
 * 
 * @author	Paul Ferraro
 * @version $Revision$
 */
public class MailTransport extends ConnectionAdapter implements MailSender
{
	public static final int DEFAULT_POOL_SIZE = 1;
	public static final MailSenderStrategy DEFAULT_SENDER_STRATEGY = new HostGroupingMailSenderStrategy();
	public static final MessageFactory DEFAULT_MESSAGE_FACTORY = new MimeMessageFactory();

	protected static Log log = LogFactory.getLog(MailTransport.class);
	
	protected Session[] sessions;
	protected List transportList = new LinkedList();
	protected ThreadGroup senderThreadGroup = new ThreadGroup("sender");
	protected ThreadGroup connectorThreadGroup = new ThreadGroup("connector");

	/**
	 * Constructs a new MailTransport using the specified session and default pool size.
	 * Same as calling <code>new MailTransport(session, MailTransport.DEFAULT_POOL_SIZE)</code>.
	 * @param session a JavaMail session
	 * @throws MessagingException if this MailTransport could not be configured.
	 */
	public MailTransport(Session session) throws MessagingException
	{
		this(session, DEFAULT_POOL_SIZE);
	}

	/**
	 * Constructs a new MailTransport using the specified session and pool size.
	 * Multiple servers can be configured by specifying a comma seperated list of servers in the
	 * <code>mail.host</code> or <code>mail.</code><em>protocol</em><code>.host</code> properties
	 * of the specified session.
	 * @param session a JavaMail session
	 * @param poolSize the number of open connections to maintain for each mail server.
	 * @throws MessagingException if this MailTransport could not be configured.
	 */
	public MailTransport(Session session, int poolSize) throws MessagingException
	{
		this(getSessions(session), poolSize);
	}

	private static Session[] getSessions(Session session) throws MessagingException
	{
		if (session == null)
		{
			throw new IllegalArgumentException("No session specified.");
		}
		
		String protocol = session.getProperties().getProperty("mail.transport.protocol", "smtp");
		String hostProperty = "mail." + protocol + ".host";
		String host = session.getProperty(hostProperty);
		
		if ((host == null) || (host.length() == 0))
		{
			hostProperty = "mail.host";
			host = session.getProperty(hostProperty);
		}
		
		if ((host == null) || (host.length() == 0))
		{
			throw new MessagingException("No transport host specified.");
		}
		
		String[] hosts = host.split(",");
		Session[] sessions = new Session[hosts.length];
		
		for (int i = 0; i < hosts.length; ++i)
		{
			Properties properties = new Properties(session.getProperties());
			
			properties.setProperty(hostProperty, hosts[i]);
			
			sessions[i] = Session.getInstance(properties);
		}
		
		return sessions;
	}
	
	/**
	 * Constructs a new MailTransport using the specified sessions and default pool size.
	 * Same as calling <code>new MailTransport(sessions, MailTransport.DEFAULT_POOL_SIZE)</code>.
	 * @param sessions an array of JavaMail sessions
	 * @throws MessagingException if this MailTransport could not be configured.
	 */
	public MailTransport(Session[] sessions) throws MessagingException
	{
		this(sessions, DEFAULT_POOL_SIZE);
	}
	
	/**
	 * Constructs a new MailTransport using the specified sessions and pool size.
	 * @param sessions an array of JavaMail sessions
	 * @param poolSize the number of open connections to maintain for each mail server.
	 * @throws MessagingException if this MailTransport could not be configured.
	 */
	public MailTransport(Session[] sessions, int poolSize) throws MessagingException
	{
		if ((sessions == null) || (sessions.length == 0))
		{
			throw new IllegalArgumentException("No sessions specified.");
		}
		
		if (poolSize < 1)
		{
			throw new IllegalArgumentException("Invalid pool size.");
		}
		
		this.sessions = sessions;
		
		for (int i = 0; i < poolSize; ++i)
		{
			for (int j = 0; j < sessions.length; ++j)
			{
				Transport transport = sessions[j].getTransport();
				
				transport.addConnectionListener(this);
				
				this.connect(transport);
			}
		}
	}
	
	/**
	 * Creates a message using the default message factory.
	 * @return a new JavaMail message
	 */
	public Message createMessage()
	{
		return this.createMessage(DEFAULT_MESSAGE_FACTORY);
	}

	/**
	 * Creates a message using the specified message factory.
	 * @param messageFactory a factory for creating JavaMail messages
	 * @return a new JavaMail message
	 */
	public Message createMessage(MessageFactory messageFactory)
	{
		return messageFactory.createMessage(this.sessions[0]);
	}
	
	/**
	 * Sends the specified message to the message's recipients using the default sender strategy.
	 * @param message a JavaMail message
	 * @throws MessagingException if sender determines that this message cannot be sent
	 */
	public void send(Message message) throws MessagingException
	{
		this.send(message, DEFAULT_SENDER_STRATEGY, null);
	}

	/**
	 * Sends the specified message to the message's recipients using the default sender strategy notifying the specified listener of the outcome.
	 * @param message a JavaMail message
	 * @param listener object to notify of the outcome of the sending of this message
	 * @throws MessagingException if sender determines that this message cannot be sent
	 */
	public void send(Message message, TransportListener listener) throws MessagingException
	{
		this.send(message, DEFAULT_SENDER_STRATEGY, listener);
	}
	
	/**
	 * Sends the specified message to the message's recipients using the specified sender strategy.
	 * @param message a JavaMail message
	 * @param strategy a sending strategy
	 * @throws MessagingException if sender determines that this message cannot be sent
	 */
	public void send(Message message, MailSenderStrategy strategy) throws MessagingException
	{
		this.send(message, strategy, null);
	}

	/**
	 * Sends the specified message to the message's recipients using the specified sender strategy notifying the specified listener of the outcome.
	 * @param message a JavaMail message
	 * @param strategy a sending strategy
	 * @param listener object to notify of the outcome of the sending of this message
	 * @throws MessagingException if sender determines that this message cannot be sent
	 */
	public void send(Message message, MailSenderStrategy strategy, TransportListener listener) throws MessagingException
	{
		strategy.send(this, message, listener);
	}
	
	/**
	 * @see net.sf.ha.mail.MailSender#send(javax.mail.Message, javax.mail.Address[], javax.mail.event.TransportListener)
	 */
	public void send(Message message, Address[] addresses, TransportListener listener)
	{
		new TransportSender(this.senderThreadGroup, message, addresses, this.nextTransport(), listener).start();
	}
	
	/**
	 * Asynchronously connects the specified transport.
	 * @param transport a JavaMail transport
	 */
	protected void connect(Transport transport)
	{
		new TransportConnector(this.connectorThreadGroup, transport).start();
	}
	
	/**
	 * Makes the specified transport available for sending.
	 * @param transport a JavaMail transport
	 */
	protected void addTransport(Transport transport)
	{
		synchronized (this.transportList)
		{
			// Make transport available
			this.transportList.add(transport);
			// Notify a thread waiting on an available transport
			this.transportList.notify();
		}
	}
	
	/**
	 * Returns the next available transport to use for sending.
	 * @return a JavaMail transport
	 */
	protected Transport nextTransport()
	{
		synchronized (this.transportList)
		{
			// If no transports are available, wait for one
			while (this.transportList.isEmpty())
			{
				log.debug("Waiting for available transport...");
				
				try
				{
					this.transportList.wait();
				}
				catch (InterruptedException e)
				{
					// Do nothing
				}
			}
			
			// Make transport unavailable
			return (Transport) this.transportList.remove(0);
		}
	}
	
	/**
	 * @see javax.mail.event.ConnectionListener#opened(javax.mail.event.ConnectionEvent)
	 */
	public void opened(ConnectionEvent event)
	{
		Transport transport = (Transport) event.getSource();
		
		log.info("Opened " + transport.getURLName().getProtocol() + " connection to " + transport.getURLName().getHost());
		
		this.addTransport(transport);
	}
	
	/**
	 * @see javax.mail.event.ConnectionListener#closed(javax.mail.event.ConnectionEvent)
	 */
	public void closed(ConnectionEvent event)
	{
		Transport transport = (Transport) event.getSource();
		
		log.info("Closed " + transport.getURLName().getProtocol() + " connection to " + transport.getURLName().getHost());
	}

	/**
	 * Closes all resources used by this MailTransport.
	 * Executing this method will interrupt any active sender or connector threads.
	 */
	public void close() 
	{
		log.info("Killing active senders...");
		
		this.senderThreadGroup.interrupt();

		log.info("Killing active connectors...");
		
		this.connectorThreadGroup.interrupt();
		
		Iterator transports = this.transportList.iterator();
		
		while (transports.hasNext())
		{
			Transport transport = (Transport) transports.next();
			
			try
			{
				transport.close();
			}
			catch (MessagingException e)
			{
				log.warn("Failed to close " + transport.getURLName().getProtocol() + " connection to " + transport.getURLName().getHost());
			}
		}
	}
	
	/**
	 * @see java.lang.Object#finalize()
	 */
	protected void finalize()
	{
		this.close();
	}
	
	/**
	 * Asynchronously (re)connect a transport and make it available.
	 */
	private static class TransportConnector extends Thread
	{
		private Transport transport;
		
		public TransportConnector(ThreadGroup threadGroup, Transport transport)
		{
			super(threadGroup, (Runnable) null);

			this.setDaemon(true);
			
			this.transport = transport;
		}
		
		public void run()
		{
			if (this.transport.isConnected())
			{
				try
				{
					this.transport.close();
				}
				catch (MessagingException e)
				{
					log.warn("Failed to close transport", e);
				}
			}
			
			while (!this.transport.isConnected() && !this.isInterrupted())
			{
				try
				{
					this.transport.connect();
				}
				catch (MessagingException e)
				{
					log.warn("Failed to connect transport", e);
					
					try
					{
						// Try again in a minute
						Thread.sleep(60000);
					}
					catch (InterruptedException ie)
					{
						this.interrupt();
					}
				}
			}
		}
	}
	
	/**
	 * Asynchronously send a message to a set of addresses via a transport.
	 */
	private class TransportSender extends Thread
	{
		private Message message;
		private Address[] addresses;
		private Transport transport;
		private TransportListener listener;
		
		public TransportSender(ThreadGroup threadGroup, Message message, Address[] addresses, Transport transport, TransportListener listener)
		{
			super(threadGroup, (Runnable) null);
			
			this.message = message;
			this.addresses = addresses;
			this.transport = transport;
			this.listener = listener;
		}
		
		public void run()
		{
			boolean retry = true;
			
			while (retry && !this.isInterrupted())
			{
				retry = false;
				
				try
				{
					if (this.listener != null)
					{
						this.transport.addTransportListener(this.listener);
					}
					
					this.transport.sendMessage(this.message, this.addresses);
				}
				catch (SendFailedException e)
				{
					log.error("Failed to send message", e);
				}
				catch (MessagingException e)
				{
					log.info("Failed to send message", e);
					
					if (this.listener != null)
					{
						this.transport.removeTransportListener(this.listener);
					}
					
					// Transport connection is dead
					// Reconnect this transport
					MailTransport.this.connect(this.transport);

					// Send again with a new transport
					this.transport = MailTransport.this.nextTransport();
					
					retry = true;
				}
				catch (RuntimeException e)
				{
					log.error("Unexpected exception", e);
				}
				catch (Error e)
				{
					log.error("Unexpected error", e);
				}
			}
			
			if (this.listener != null)
			{
				this.transport.removeTransportListener(this.listener);
			}
			
			// Make transport available again
			MailTransport.this.addTransport(this.transport);
		}
	}
}
