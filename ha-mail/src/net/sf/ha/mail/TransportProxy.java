package net.sf.ha.mail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.mail.Address;
import javax.mail.Message;
import javax.mail.MessagingException;
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
public class TransportProxy extends Transport implements Sender, ConnectionListener
{
	public static final String POOL_SIZE = "mail.transport.pool-size";
	public static final String SENDER_STRATEGY = "mail.transport.sender-strategy";
	public static final String CONNECT_RETRY_PERIOD = "mail.transport.connect-retry-period";
	
	private static final String DEFAULT_SENDER_STRATEGY = SimpleSenderStrategy.class.getName();
	private static final String DEFAULT_TRANSPORT_PROTOCOL = "smtp";
	private static final int DEFAULT_CONNECT_RETRY_PERIOD = 60;
	private static final int DEFAULT_POOL_SIZE = 1;
	
	protected static Log log = LogFactory.getLog(TransportProxy.class);

	protected List transportList;
	protected List activeTransportList = new LinkedList();
	protected ThreadGroup senderThreadGroup = new ThreadGroup("sender");
	protected ThreadGroup connectorThreadGroup = new ThreadGroup("connector");
	protected long connectRetryPeriod;
	protected Map urlNameMap = new HashMap();
	private int poolSize;
	private Provider provider;
	private SenderStrategy senderStrategy = new SimpleSenderStrategy();
	
	/**
	 * Constructs a new TransportProxy.
	 * @param session
	 * @param url
	 */
	public TransportProxy(Session session, URLName url) throws MessagingException
	{
		super(session, url);

		Properties properties = session.getProperties();
		
		this.poolSize = Integer.parseInt(properties.getProperty(POOL_SIZE, Integer.toString(DEFAULT_POOL_SIZE)));
		this.connectRetryPeriod = 1000 * Integer.parseInt(properties.getProperty(CONNECT_RETRY_PERIOD, Integer.toString(DEFAULT_CONNECT_RETRY_PERIOD)));
		
		String protocol = properties.getProperty("mail.transport.protocol", DEFAULT_TRANSPORT_PROTOCOL);
		String hostProperty = "mail." + protocol + ".host";
		String host = properties.getProperty(hostProperty);
		
		if ((host == null) || (host.length() == 0))
		{
			hostProperty = "mail.host";
			host = properties.getProperty(hostProperty);
		}
		
		if ((host == null) || (host.length() == 0))
		{
			throw new MessagingException("No transport host specified.");
		}
		
		String senderStrategyClassName = properties.getProperty(SENDER_STRATEGY, DEFAULT_SENDER_STRATEGY);
		
		try
		{
			Class senderStrategyClass = Class.forName(senderStrategyClassName);
			Object senderStrategy = senderStrategyClass.newInstance();
			
			if (!SenderStrategy.class.isInstance(senderStrategy))
			{
				throw new MessagingException("Sender strategry " + senderStrategyClassName + " does not implement " + SenderStrategy.class.getName());
			}
			
			this.senderStrategy = (SenderStrategy) senderStrategy;
		}
		catch (ClassNotFoundException e)
		{
			throw new MessagingException("Invalid sender strategy: " + senderStrategyClassName, e);
		}
		catch (InstantiationException e)
		{
			throw new MessagingException("Failed to create sender strategy: " + senderStrategyClassName, e);
		}
		catch (IllegalAccessException e)
		{
			throw new MessagingException("Invalid sender strategy: " + senderStrategyClassName, e);
		}
		
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
			throw new MessagingException("Could not find an appropriate " + url.getProtocol() + " provider.");
		}
		
		String[] hosts = host.split(",");
		
		this.transportList = new ArrayList(this.poolSize * hosts.length);
		
		for (int i = 0; i < this.poolSize; ++i)
		{
			for (int j = 0; j < hosts.length; ++j)
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
	public void sendMessage(Message message, Address[] addresses) throws MessagingException
	{
		if (!this.isConnected())
		{
			throw new MessagingException("Transport not connected");
		}
		
		if ((addresses == null) || (addresses.length == 0))
		{
			// Nobody will recieve this message
			return;
		}
		
		if (message.getSubject() == null)
		{
			message.setSubject("");
		}

		Address[] recipients = message.getAllRecipients();
		
		if ((recipients == null) || (recipients.length == 0))
		{
			throw new MessagingException("Message contains no recipients.");
		}
		
		try
		{
			if (message.getContent() == null)
			{
				message.setText("");
			}
		}
		catch (IOException e)
		{
			throw new MessagingException("Failed to get message content", e);
		}
		
		this.senderStrategy.send(this, message, addresses);
	}
	
	/**
	 * Creates and starts a new sender thread using the next available underlying transport.
	 * @see net.sf.ha.mail.Sender#send(javax.mail.Transport, javax.mail.Message, javax.mail.Address[])
	 */
	public void send(Transport transport, Message message, Address[] addresses)
	{
		new SenderThread(transport, message, addresses).start();
	}
	
	/**
	 * Creates and starts a new connector thread for each underlying transport.
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

			new ConnectorThread(transport, this.getURLName(host)).start();
		}
	
		return true;
	}

	/**
	 * @param host
	 * @return
	 */
	protected URLName getURLName(String host)
	{
		return (URLName) this.urlNameMap.get(host);
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
	 * @see net.sf.ha.mail.Sender#nextAvailableTransport()
	 */
	public Transport nextAvailableTransport()
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
	 * Closes the Transport proxy.  Implementation is as follows:
	 * <ol>
	 *  <li>Waits until until all sender threads have completed.</li>
	 * 	<li>Interrupts the execution of any active connector threads</li>
	 * 	<li>Closes the underlying transports</li>
	 *  <li>Calls <code>javax.mail.Service.close()</code></li>
	 * </ol>
	 * @see javax.mail.Service#close()
	 */
	public void close() throws MessagingException
	{
		while (this.senderThreadGroup.activeCount() > 0)
		{
			Thread.yield();
		}
		
		if (this.connectorThreadGroup.activeCount() > 0)
		{
			this.connectorThreadGroup.interrupt();
		}
		
		for (int i = 0; i < this.transportList.size(); ++i)
		{
			Transport transport = (Transport) this.transportList.get(i);
			
			if (transport.isConnected())
			{
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
		
		super.close();
	}
	
	protected void finalize() throws Throwable
	{
		if (this.isConnected())
		{
			this.close();
		}
		
		super.finalize();
	}
	
	/**
	 * Asynchronously (re)connect a transport and make it available.
	 */
	private class ConnectorThread extends Thread
	{
		private Transport transport;
		private URLName url;
		
		public ConnectorThread(Transport transport, URLName url)
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
						// Try again after a delay
						Thread.sleep(TransportProxy.this.connectRetryPeriod);
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
	private class SenderThread extends Thread
	{
		private Transport transport;
		private Message message;
		private Address[] addresses;
		
		public SenderThread(Transport transport, Message message, Address[] addresses)
		{
			super(TransportProxy.this.senderThreadGroup, (Runnable) null);

			this.setDaemon(true);

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
					String host = this.transport.getURLName().getHost();
					URLName url = TransportProxy.this.getURLName(host);
					new ConnectorThread(this.transport, url).start();

					// Try again with a new transport
					this.transport = TransportProxy.this.nextAvailableTransport();
					
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
