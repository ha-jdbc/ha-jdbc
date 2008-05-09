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

import java.text.MessageFormat;
import java.util.Collection;
import java.util.Set;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseEvent;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.StateManager;

import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.Message;
import org.jgroups.MessageListener;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.util.Rsp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StateManager implementation that broadcasts database activations and deactivations to other group members
 * and retrieves initial state from another group member.
 * 
 * @author Paul Ferraro
 */
public class DistributableStateManager extends AbstractMembershipListener implements StateManager, MessageListener, RequestHandler
{
	private static final String CHANNEL = "{0}-state"; //$NON-NLS-1$
	
	private static Logger logger = LoggerFactory.getLogger(DistributableStateManager.class);
	
	private int timeout;
	private MessageDispatcher dispatcher;
	private DatabaseCluster<?> databaseCluster;
	private StateManager stateManager;
	
	/**
	 * @param databaseCluster
	 * @param decorator
	 * @throws Exception
	 */
	public DistributableStateManager(DatabaseCluster<?> databaseCluster, DistributableDatabaseClusterDecorator decorator) throws Exception
	{
		super(decorator.createChannel(MessageFormat.format(CHANNEL, databaseCluster.getId())));
		
		this.databaseCluster = databaseCluster;
		
		this.dispatcher = new MessageDispatcher(this.channel, this, this, this);

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
			Command<Object> command = (Command) message.getObject();
	
			logger.info(Messages.getMessage(Messages.COMMAND_RECEIVED, command));
			
			return command.marshalResult(command.execute(this.databaseCluster, this.stateManager));
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
	@Override
	public Set<String> getInitialState()
	{
		Command<Set<String>> command = new QueryInitialStateCommand();

		Collection<Rsp> responses = this.send(command, GroupRequest.GET_FIRST, this.timeout);
		
		for (Rsp response: responses)
		{
			Object result = response.getValue();
			
			if (result != null)
			{
				Set<String> state = command.unmarshalResult(result);
				
				logger.info(Messages.getMessage(Messages.INITIAL_CLUSTER_STATE_REMOTE, state, response.getSender()));
				
				return state;
			}
		}

		return this.stateManager.getInitialState();
	}

	/**
	 * @see net.sf.hajdbc.DatabaseActivationListener#activated(net.sf.hajdbc.DatabaseEvent)
	 */
	@Override
	public void activated(DatabaseEvent event)
	{
		if (this.databaseCluster.isActive())
		{
			// Send synchronous notification
			this.send(new ActivateCommand(event.getId()), GroupRequest.GET_ALL, 0);
		}
		
		this.stateManager.activated(event);
	}

	/**
	 * @see net.sf.hajdbc.DatabaseDeactivationListener#deactivated(net.sf.hajdbc.DatabaseEvent)
	 */
	@Override
	public void deactivated(DatabaseEvent event)
	{
		// Send asynchronous notification
		this.send(new DeactivateCommand(event.getId()), GroupRequest.GET_NONE, this.timeout);
		
		this.stateManager.deactivated(event);
	}

	private Collection<Rsp> send(Command<?> command, int mode, long timeout)
	{
		return this.dispatcher.castMessage(null, this.createMessage(command), mode, timeout).values();
	}
	
	private Message createMessage(Command<?> command)
	{
		return new Message(null, this.dispatcher.getChannel().getLocalAddress(), command);
	}
	
	/**
	 * @see net.sf.hajdbc.StateManager#start()
	 */
	@Override
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
	@Override
	public void stop()
	{
		this.dispatcher.stop();
		
		this.dispatcher.getChannel().close();
		
		this.stateManager.stop();
	}

	/**
	 * @see net.sf.hajdbc.distributable.AbstractMembershipListener#memberJoined(org.jgroups.Address)
	 */
	@Override
	protected void memberJoined(Address address)
	{
		logger.info(Messages.getMessage(Messages.GROUP_MEMBER_JOINED, address, this.databaseCluster));
	}

	/**
	 * @see net.sf.hajdbc.distributable.AbstractMembershipListener#memberLeft(org.jgroups.Address)
	 */
	@Override
	protected void memberLeft(Address address)
	{
		logger.info(Messages.getMessage(Messages.GROUP_MEMBER_LEFT, address, this.databaseCluster));
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
