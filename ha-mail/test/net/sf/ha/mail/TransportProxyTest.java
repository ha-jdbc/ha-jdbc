package net.sf.ha.mail;

import java.text.DecimalFormat;
import java.util.Properties;

import javax.mail.Address;
import javax.mail.Message;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.event.TransportEvent;
import javax.mail.event.TransportListener;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

/**
 * @author	Paul Ferraro
 * @version $Revision$
 * @since	1.0
 */
public class TransportProxyTest implements TransportListener
{
	private boolean debug = false;
	
	public static void main(String[] args)
	{
		try
		{
			if (args.length != 5)
			{
				System.err.println("Usage: java " + TransportProxyTest.class.getName() + " <comma-seperated-list-of-smtp-servers> <connection-pool-size> <comma-seperated-list-of-email-addresses> <message-count> <debug-mode?>");
				System.exit(1);
			}
			
			String servers = args[0];
			int poolSize = Integer.parseInt(args[1]);
			Address[] addresses = InternetAddress.parse(args[2]);
			int count = Integer.parseInt(args[3]);
			
			Properties properties = new Properties();
			properties.setProperty("mail.transport.protocol", "smtp");
			properties.setProperty("mail.host", servers);
			properties.setProperty("mail.transport.pool-size", Integer.toString(poolSize));
			
			TransportProxyTest mailTest = new TransportProxyTest();
			Session session = Session.getInstance(properties);

			if (args.length == 5)
			{
				mailTest.debug = Boolean.valueOf(args[4]).booleanValue();
			}

			Transport transport = session.getTransport();
			
			transport.addTransportListener(mailTest);
			
			long startTime = System.currentTimeMillis();
			
			transport.connect();
			
			for (int i = 0; i < count; ++i)
			{
				Message message = new MimeMessage(session);
				message.setRecipients(Message.RecipientType.TO, addresses);
				message.setSubject("TEST");
				message.setText("");
				
				transport.sendMessage(message, message.getAllRecipients());
			}

			transport.close();
			
			long executionTime = System.currentTimeMillis() - startTime;
			
			System.out.println("Total execution time = " + new DecimalFormat("#0").format(executionTime / 60000) + ":" + new DecimalFormat("00").format((executionTime / 1000) % 60) + "." + new DecimalFormat("000").format(executionTime % 1000));
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.err);
		}
	}

	/**
	 * @see javax.mail.event.TransportListener#messageDelivered(javax.mail.event.TransportEvent)
	 */
	public void messageDelivered(TransportEvent event)
	{
		logEvent(event);
	}

	/**
	 * @see javax.mail.event.TransportListener#messageNotDelivered(javax.mail.event.TransportEvent)
	 */
	public void messageNotDelivered(TransportEvent event)
	{
		logEvent(event);
	}

	/**
	 * @see javax.mail.event.TransportListener#messagePartiallyDelivered(javax.mail.event.TransportEvent)
	 */
	public void messagePartiallyDelivered(TransportEvent event)
	{
		logEvent(event);
	}
	
	private void logEvent(TransportEvent event)
	{
		if (this.debug)
		{
			if ((event.getInvalidAddresses() != null) && (event.getInvalidAddresses().length > 0))
			{
				System.out.println("Invalid email address(es): " + InternetAddress.toString(event.getInvalidAddresses()) + " on " + ((javax.mail.Transport) event.getSource()).getURLName().toString());
			}
			
			if ((event.getValidSentAddresses() != null) && (event.getValidSentAddresses().length > 0))
			{
				System.out.println("Successfully sent message to: " + InternetAddress.toString(event.getValidSentAddresses()) + " on " + ((javax.mail.Transport) event.getSource()).getURLName().toString());
			}
			
			if ((event.getValidUnsentAddresses() != null) && (event.getValidUnsentAddresses().length > 0))
			{
				System.out.println("Failed to send message to: " + InternetAddress.toString(event.getValidUnsentAddresses()) + " on " + ((javax.mail.Transport) event.getSource()).getURLName().toString());
			}
		}
	}
}
