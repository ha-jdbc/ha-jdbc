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
import javax.mail.internet.MimeMessage;
import javax.naming.Context;
import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Manages an email server connection pool of configurable size using a multi-threaded blocking queue.
 * 
 * @author	Paul Ferraro
 * @version $Revision$
 */
public class MailTransport extends ConnectionAdapter implements MailSender
{
	public static final MailSenderStrategy DEFAULT_STRATEGY = new HostGroupingMailSenderStrategy();
	public static final int DEFAULT_POOL_SIZE = 1;

	protected static Log log = LogFactory.getLog(MailTransport.class);
	
	protected Session[] sessions;
	protected List transportList = new LinkedList();
	protected ThreadGroup senderThreadGroup = new ThreadGroup("sender");
	protected ThreadGroup connectorThreadGroup = new ThreadGroup("connector");
	
	private static Session[] createSessions(Context context, String[] names) throws NamingException
	{
		Session[] sessions = new Session[names.length];
		
		for (int i = 0; i < names.length; ++i)
		{
			String name = names[i];
			Object object = context.lookup(name);
			
			if (!Session.class.isInstance(object))
			{
				throw new NamingException(name + " refers to " + object.getClass().getName() + ", expected " + Session.class.getName());
			}
			
			sessions[i] = (Session) object;
		}
		
		return sessions;
	}
	
	private static Session[] createSessions(Properties[] properties)
	{
		Session[] sessions = new Session[properties.length];
		
		for (int i = 0; i < properties.length; ++i)
		{
			sessions[i] = Session.getInstance(properties[i]);
		}
		
		return sessions;
	}
	
	public MailTransport(Context context, String name) throws NamingException, MessagingException 
	{
		this(context, new String[] { name }, DEFAULT_POOL_SIZE);
	}
	
	public MailTransport(Context context, String name, int poolSize) throws NamingException, MessagingException 
	{
		this(context, new String[] { name }, poolSize);
	}
	
	public MailTransport(Context context, String[] names) throws NamingException, MessagingException 
	{
		this(context, names, DEFAULT_POOL_SIZE);
	}
	
	public MailTransport(Context context, String[] names, int poolSize) throws NamingException, MessagingException 
	{
		this(createSessions(context, names), poolSize);
	}
	
	public MailTransport(Properties properties) throws MessagingException 
	{
		this(new Properties[] { properties }, DEFAULT_POOL_SIZE);
	}
	
	public MailTransport(Properties properties, int poolSize) throws MessagingException 
	{
		this(new Properties[] { properties }, poolSize);
	}
	
	public MailTransport(Properties[] properties) throws MessagingException 
	{
		this(properties, DEFAULT_POOL_SIZE);
	}

	public MailTransport(Properties[] properties, int poolSize) throws MessagingException 
	{
		this(createSessions(properties), poolSize);
	}

	public MailTransport(Session session, int poolSize) throws MessagingException
	{
		this(new Session[] { session }, poolSize);
	}
	
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
	
	public Message createMessage()
	{
		return new MimeMessage(this.sessions[0]);
	}
	
	public void send(Message message) throws MessagingException
	{
		this.send(message, DEFAULT_STRATEGY, null);
	}

	public void send(Message message, TransportListener listener) throws MessagingException
	{
		this.send(message, DEFAULT_STRATEGY, listener);
	}
	
	public void send(Message message, MailSenderStrategy strategy) throws MessagingException
	{
		this.send(message, strategy, null);
	}

	public void send(Message message, MailSenderStrategy strategy, TransportListener listener) throws MessagingException
	{
		strategy.send(this, message, listener);
	}
	
	public void send(Message message, Address[] addresses, TransportListener listener)
	{
		new TransportSender(this.senderThreadGroup, message, addresses, listener).start();
	}
	
	protected void connect(Transport transport)
	{
		new TransportConnector(this.connectorThreadGroup, transport).start();
	}
	
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
		
		log.info(transport.getURLName().getProtocol() + " connection made to " + transport.getURLName().getHost());
		
		this.addTransport(transport);
	}
	
	/**
	 * @see javax.mail.event.ConnectionListener#closed(javax.mail.event.ConnectionEvent)
	 */
	public void closed(ConnectionEvent event)
	{
		Transport transport = (Transport) event.getSource();
		
		log.info(transport.getURLName().getProtocol() + " connection closed to " + transport.getURLName().getHost());
	}

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
				
				log.info("Closed " + transport.getURLName().getProtocol() + " connection to " + transport.getURLName().getHost());
			}
			catch (MessagingException e)
			{
				log.warn("Failure closing " + transport.getURLName().getProtocol() + " connection to " + transport.getURLName().getHost());
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
		private TransportListener listener;
		
		public TransportSender(ThreadGroup threadGroup, Message message, Address[] addresses, TransportListener listener)
		{
			super(threadGroup, (Runnable) null);
			
			this.message = message;
			this.addresses = addresses;
			this.listener = listener;
		}
		
		public void run()
		{
			Transport transport = MailTransport.this.nextTransport();
			
			boolean retry = true;
			
			while (retry && !this.isInterrupted())
			{
				retry = false;
				
				try
				{
					if (this.listener != null)
					{
						transport.addTransportListener(this.listener);
					}
					
					transport.sendMessage(this.message, this.addresses);
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
						transport.removeTransportListener(this.listener);
					}
					
					// Transport connection is dead
					// Reconnect this transport
					MailTransport.this.connect(transport);

					// Send again with a new transport
					transport = MailTransport.this.nextTransport();
					
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
				transport.removeTransportListener(this.listener);
			}
			
			// Make transport available again
			MailTransport.this.addTransport(transport);
		}
	}
}
