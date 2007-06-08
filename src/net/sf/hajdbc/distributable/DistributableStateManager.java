/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2007 Paul Ferraro
 * 
 * This library is free software; you can redistribute it and/or modify it 
 * under the terms of the GNU Lesser General Public License as published by the 
 * Free Software Foundation; either version 2.1 of the License, or (at your 
 * option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License 
 * for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation, 
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 * 
 * Contact: ferraro@users.sourceforge.net
 */
package net.sf.hajdbc.distributable;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.StateManager;

import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.MembershipListener;
import org.jgroups.Message;
import org.jgroups.MessageListener;
import org.jgroups.SuspectedException;
import org.jgroups.TimeoutException;
import org.jgroups.View;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StateManager implementation that broadcasts database activations and deactivations to other group members
 * and retrieves initial state from another group member.
 * 
 * @author Paul Ferraro
 */
public class DistributableStateManager implements StateManager, MessageListener, MembershipListener, RequestHandler
{
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	
	private int timeout;
	private MessageDispatcher dispatcher;
	private DatabaseCluster<?> databaseCluster;
	private StateManager stateManager;
	private List<Address> addressList = new LinkedList<Address>();
	
	public DistributableStateManager(DatabaseCluster<?> databaseCluster, DistributableDatabaseClusterDecorator decorator) throws Exception
	{
		this.databaseCluster = databaseCluster;
		
		this.dispatcher = new MessageDispatcher(decorator.createChannel(databaseCluster.getId() + "-state"), this, this, this);
		
		this.timeout = decorator.getTimeout();
		this.stateManager = databaseCluster.getStateManager();
	}

	/**
	 * @see org.jgroups.blocks.RequestHandler#handle(org.jgroups.Message)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Object handle(Message message)
	{
		try
		{
			Command<Object> command = Command.class.cast(message.getObject());
	
			this.logger.info(Messages.getMessage(Messages.COMMAND_RECEIVED, command));
			
			return command.marshalResult(command.execute(this.databaseCluster));
		}
		catch (Throwable e)
		{
			logger.error(e.getMessage(), e);
			
			return e;
		}
	}

	/**
	 * @see net.sf.hajdbc.StateManager#getInitialState()
	 */
	public Set<String> getInitialState()
	{
		Address coordinator = this.getCoordinator();
		
		if (coordinator.equals(this.dispatcher.getChannel().getLocalAddress()))
		{
			return this.stateManager.getInitialState();
		}

		Command<Set<String>> command = new QueryInitialStateCommand();

		Message message = new Message(coordinator, this.dispatcher.getChannel().getLocalAddress(), command);
		
		try
		{
			Object result = this.dispatcher.sendMessage(message, GroupRequest.GET_FIRST, this.timeout);

			return command.unmarshalResult(String.class.cast(result));
		}
		catch (TimeoutException e)
		{
			return this.stateManager.getInitialState();
		}
		catch (SuspectedException e)
		{
			return this.stateManager.getInitialState();
		}
	}
	
	private Address getCoordinator()
	{
		synchronized (this.addressList)
		{
			return this.addressList.isEmpty() ? this.dispatcher.getChannel().getLocalAddress() : this.addressList.get(0);
		}
	}
	
	/**
	 * @see net.sf.hajdbc.StateManager#add(java.lang.String)
	 */
	public void add(String databaseId)
	{
		this.send(new ActivateCommand(databaseId));
		
		this.stateManager.add(databaseId);
	}

	/**
	 * @see net.sf.hajdbc.StateManager#remove(java.lang.String)
	 */
	public void remove(String databaseId)
	{
		this.send(new DeactivateCommand(databaseId));
		
		this.stateManager.remove(databaseId);
	}

	private void send(Command<?> command)
	{
		Message message = new Message(null, null, command);

		this.dispatcher.castMessage(null, message, GroupRequest.GET_ALL, this.timeout);
	}

	/**
	 * @see net.sf.hajdbc.StateManager#start()
	 */
	public void start() throws Exception
	{
		Channel channel = this.dispatcher.getChannel();
		
		channel.connect(channel.getClusterName());

		this.dispatcher.start();
		
		this.stateManager.start();
	}

	/**
	 * @see net.sf.hajdbc.StateManager#stop()
	 */
	public void stop()
	{
		this.dispatcher.stop();
		
		this.dispatcher.getChannel().close();
		
		this.stateManager.stop();
	}

	/**
	 * @see org.jgroups.MembershipListener#block()
	 */
	@Override
	public void block()
	{
		// Ignore
	}

	/**
	 * @see org.jgroups.MembershipListener#suspect(org.jgroups.Address)
	 */
	@Override
	public void suspect(Address address)
	{
		// Ignore
	}

	/**
	 * @see org.jgroups.MembershipListener#viewAccepted(org.jgroups.View)
	 */
	@Override
	public void viewAccepted(View view)
	{
		Vector<Address> addresses = view.getMembers();
		
		synchronized (this.addressList)
		{
			for (Address address: this.addressList)
			{
				if (!view.containsMember(address))
				{
					logger.info(Messages.getMessage(Messages.GROUP_MEMBER_LEFT, address, this.databaseCluster));
				}
			}

			for (Address address: addresses)
			{
				if (!this.addressList.contains(address))
				{
					logger.info(Messages.getMessage(Messages.GROUP_MEMBER_JOINED, address, this.databaseCluster));
				}
			}
			
			this.addressList.clear();
			this.addressList.addAll(addresses);
		}
	}

	/**
	 * @see org.jgroups.MessageListener#getState()
	 */
	@Override
	public byte[] getState()
	{
		return null;
	}

	/**
	 * @see org.jgroups.MessageListener#setState(byte[])
	 */
	@Override
	public void setState(byte[] state)
	{
		// Do nothing
	}

	/**
	 * @see org.jgroups.MessageListener#receive(org.jgroups.Message)
	 */
	@Override
	public void receive(Message message)
	{
		// Do nothing
	}
}
