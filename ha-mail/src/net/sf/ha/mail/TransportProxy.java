package net.sf.ha.mail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.mail.Address;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.NoSuchProviderException;
import javax.mail.Provider;
import javax.mail.SendFailedException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.URLName;
import javax.mail.event.ConnectionEvent;
import javax.mail.event.ConnectionListener;
import javax.mail.event.TransportListener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class TransportProxy extends Transport implements ConnectionListener
{
	public static final String POOL_SIZE = "transport.pool.size";
	private static final int DEFAULT_POOL_SIZE = 1;
	
	protected static Log log = LogFactory.getLog(TransportProxy.class);

	protected List transportList;
	protected List activeTransportList = new LinkedList();
	protected ThreadGroup senderThreadGroup = new ThreadGroup("sender");
	protected ThreadGroup connectorThreadGroup = new ThreadGroup("connector");
	private int poolSize = DEFAULT_POOL_SIZE;
	protected Map urlNameMap = new HashMap();
	private Provider provider;
	
	/**
	 * Constructs a new Transport.
	 * @param session
	 * @param url
	 */
	public TransportProxy(Session session, URLName url) throws MessagingException
	{
		super(session, url);
		
		String poolSizeProperty = session.getProperty(POOL_SIZE);
		
		if (poolSizeProperty != null)
		{
			this.poolSize = Integer.parseInt(poolSizeProperty);
		}
/*		
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
*/		
		Provider[] providers = session.getProviders();
		
		for (int i = 0; i < providers.length; ++i)
		{
			if (providers[i].getType().equals(Provider.Type.TRANSPORT))
			{
				if (providers[i].getProtocol().equals(url.getProtocol()))
				{
					if (!providers[i].getClassName().equals(this.getClass().getName()))
					{
						this.provider = providers[i];
						break;
					}
				}
			}
		}
		
		if (this.provider == null)
		{
			throw new NoSuchProviderException("Could not find an appropriate " + url.getProtocol() + " provider.");
		}
		
		String[] hosts = url.getHost().split(",");
		this.transportList = new ArrayList(this.poolSize * hosts.length);
		
		for (int j = 0; j < this.poolSize; ++j)
		{
			for (int i = 0; i < hosts.length; ++i)
			{
				Transport transport = this.session.getTransport(this.provider);
				
				transport.addConnectionListener(this);
				
				this.transportList.add(transport);
			}
		}
	}
	
	/**
	 * @see javax.mail.Transport#sendMessage(javax.mail.Message, javax.mail.Address[])
	 */
	public void sendMessage(Message message, Address[] addresses)
	{
		new TransportSender(this.nextTransport(), message, addresses).start();
	}
	
	/**
	 * @see javax.mail.Service#protocolConnect(java.lang.String, int, java.lang.String, java.lang.String)
	 */
	protected boolean protocolConnect(String hostList, int port, String user, String password)
	{
		String[] hosts = hostList.split(",");
		
		for (int i = 0; i < hosts.length; ++i)
		{
			String host = hosts[i];
			
			URLName url = new URLName(this.url.getProtocol(), host, port, this.url.getFile(), user, password);
			
			this.urlNameMap.put(host, url);
		}
		
		for (int i = 0; i < this.transportList.size(); ++i)
		{
			Transport transport = (Transport) this.transportList.get(i);
			String host = hosts[i % hosts.length];

			this.connect(transport, this.getURLName(host));
		}
	
		return true;
	}

	protected URLName getURLName(String host)
	{
		return (URLName) this.urlNameMap.get(host);
	}
	
	/**
	 * Asynchronously connects the specified transport.
	 * @param transport a JavaMail transport
	 */
	protected void connect(Transport transport, URLName url)
	{
		new TransportConnector(transport, url).start();
	}
	
	/**
	 * Make the specified transport available for sending.
	 * @param transport a JavaMail transport
	 */
	protected void addTransport(javax.mail.Transport transport)
	{
		synchronized (this.activeTransportList)
		{
			// Make transport available
			this.activeTransportList.add(transport);
			// Notify a thread waiting on an available transport
			this.activeTransportList.notify();
		}
	}
	
	/**
	 * Returns the next available transport to use for sending.
	 * @return a JavaMail transport
	 */
	protected Transport nextTransport()
	{
		synchronized (this.activeTransportList)
		{
			// If no transports are available, wait for one
			while (this.activeTransportList.isEmpty())
			{
				log.debug("Waiting for available transport...");
				
				try
				{
					this.activeTransportList.wait();
				}
				catch (InterruptedException e)
				{
					// Do nothing
				}
			}
			
			// Make transport unavailable
			return (Transport) this.activeTransportList.remove(0);
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
	 * @see javax.mail.event.ConnectionListener#disconnected(javax.mail.event.ConnectionEvent)
	 */
	public void disconnected(ConnectionEvent event)
	{
		Transport transport = (Transport) event.getSource();
		
		log.debug("Disconnected " + transport.getURLName().getProtocol() + " connection from " + transport.getURLName().getHost());
	}

	/**
	 * @see javax.mail.Transport#addTransportListener(javax.mail.event.TransportListener)
	 */
	public void addTransportListener(TransportListener listener)
	{
		for (int i = 0; i < this.transportList.size(); ++i)
		{
			Transport transport = (Transport) this.transportList.get(i);
			
			transport.addTransportListener(listener);
		}
	}

	/**
	 * @see javax.mail.Transport#removeTransportListener(javax.mail.event.TransportListener)
	 */
	public void removeTransportListener(TransportListener listener)
	{
		for (int i = 0; i < this.transportList.size(); ++i)
		{
			Transport transport = (Transport) this.transportList.get(i);
			
			transport.removeTransportListener(listener);
		}
	}
	
	/**
	 * @see javax.mail.Service#addConnectionListener(javax.mail.event.ConnectionListener)
	 */
	public void addConnectionListener(ConnectionListener listener)
	{
		for (int i = 0; i < this.transportList.size(); ++i)
		{
			Transport transport = (Transport) this.transportList.get(i);
				
			transport.addConnectionListener(listener);
		}
	}
	
	/**
	 * @see javax.mail.Service#removeConnectionListener(javax.mail.event.ConnectionListener)
	 */
	public void removeConnectionListener(ConnectionListener listener)
	{
		for (int i = 0; i < this.transportList.size(); ++i)
		{
			Transport transport = (Transport) this.transportList.get(i);
			
			transport.removeConnectionListener(listener);
		}
	}
	
	/**
	 * @see javax.mail.Service#close()
	 */
	public void close()
	{
		while (this.senderThreadGroup.activeCount() > 0)
		{
			Thread.yield();
		}
		
		log.info("Killing active connectors...");
		
		this.connectorThreadGroup.interrupt();
		
		for (int i = 0; i < this.transportList.size(); ++i)
		{
			Transport transport = (Transport) this.transportList.get(i);
			
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
	
	protected void finalize()
	{
		this.close();
	}
	
	/**
	 * Asynchronously (re)connect a transport and make it available.
	 */
	private class TransportConnector extends Thread
	{
		private Transport transport;
		private URLName url;
		
		public TransportConnector(Transport transport, URLName url)
		{
			super(TransportProxy.this.connectorThreadGroup, (Runnable) null);

			this.setDaemon(true);
			
			this.transport = transport;
			this.url = url;
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
					this.transport.connect(this.url.getHost(), this.url.getPort(), this.url.getUsername(), this.url.getPassword());
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
		private Transport transport;
		private Message message;
		private Address[] addresses;
		
		public TransportSender(Transport transport, Message message, Address[] addresses)
		{
			super(TransportProxy.this.senderThreadGroup, (Runnable) null);
			
			this.transport = transport;
			this.message = message;
			this.addresses = addresses;
		}
		
		public void run()
		{
			boolean retry = true;
			
			while (retry && !this.isInterrupted())
			{
				retry = false;
				
				try
				{
					this.transport.sendMessage(this.message, this.addresses);
				}
				catch (SendFailedException e)
				{
					log.error("Failed to send message", e);
				}
				catch (MessagingException e)
				{
					log.info("Failed to send message", e);
					
					// Transport connection is dead
					// Reconnect this transport
					URLName url = TransportProxy.this.getURLName(this.transport.getURLName().getHost());
					TransportProxy.this.connect(this.transport, url);

					// Try again with a new transport
					this.transport = TransportProxy.this.nextTransport();
					
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
			
			// Make transport available again
			TransportProxy.this.addTransport(this.transport);
		}
	}
}
