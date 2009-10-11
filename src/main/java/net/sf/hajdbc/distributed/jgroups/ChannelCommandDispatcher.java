/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2009 Paul Ferraro
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
package net.sf.hajdbc.distributed.jgroups;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import net.sf.hajdbc.Messages;
import net.sf.hajdbc.distributed.Command;
import net.sf.hajdbc.distributed.CommandDispatcher;
import net.sf.hajdbc.distributed.Member;
import net.sf.hajdbc.distributed.MembershipListener;
import net.sf.hajdbc.distributed.Stateful;
import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggerFactory;

import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.ExtendedMembershipListener;
import org.jgroups.Message;
import org.jgroups.MessageListener;
import org.jgroups.SuspectedException;
import org.jgroups.TimeoutException;
import org.jgroups.View;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.util.Rsp;

/**
 * @author paul
 *
 */
public class ChannelCommandDispatcher<C> implements RequestHandler, CommandDispatcher<C>, ExtendedMembershipListener, MessageListener
{
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	private final String id;
	private final MessageDispatcher dispatcher;
	private final C context;
	private final AtomicReference<View> viewReference = new AtomicReference<View>();
	private final MembershipListener membershipListener;
	private final Stateful stateful;
	
	public ChannelCommandDispatcher(String id, ChannelProvider provider, C context, Stateful stateful, MembershipListener membershipListener) throws Exception
	{
		this.id = id;
		this.context = context;
		this.stateful = stateful;
		this.membershipListener = membershipListener;
		
		this.dispatcher = new MessageDispatcher(provider.getChannel(), this, this, this);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.Lifecycle#start()
	 */
	@Override
	public void start() throws Exception
	{
		Channel channel = this.dispatcher.getChannel();
		
		channel.setOpt(Channel.LOCAL, true);
		
		channel.connect(this.id, null, null, 0);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.Lifecycle#stop()
	 */
	@Override
	public void stop()
	{
		Channel channel = this.dispatcher.getChannel();
		
		if (channel.isOpen())
		{
			if (channel.isConnected())
			{
				channel.disconnect();
			}
			
			channel.close();
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.distributable.jgroups.ICommandDispatcher#executeAll(net.sf.hajdbc.distributed.Command)
	 */
	public <R> Map<Member, R> executeAll(Command<R, C> command)
	{
		Message message = new Message(null, this.getLocalAddress(), command);
		
		Map<Address, Rsp> responses = this.dispatcher.castMessage(null, message, GroupRequest.GET_ALL, 0);
		
		if (responses == null) return Collections.emptyMap();
		
		Map<Member, R> results = new TreeMap<Member, R>();
		
		for (Map.Entry<Address, Rsp> entry: responses.entrySet())
		{
			Rsp response = entry.getValue();

			if (response.wasReceived() && !response.wasSuspected())
			{
				results.put(new AddressMember(entry.getKey()), command.unmarshalResult(response.getValue()));
			}
		}
		
		return results;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.distributable.jgroups.ICommandDispatcher#executeCoordinator(net.sf.hajdbc.distributed.Command)
	 */
	public <R> R executeCoordinator(Command<R, C> command)
	{
		while (true)
		{
			Message message = new Message(this.getCoordinatorAddress(), this.getLocalAddress(), command);

			try
			{
				Object result = this.dispatcher.sendMessage(message, GroupRequest.GET_ALL, 0);
				
				return command.unmarshalResult(result);
			}
			catch (TimeoutException e)
			{
				throw new IllegalStateException(e);
			}
			catch (SuspectedException e)
			{
				// log
			}
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.distributable.jgroups.CommandDispatcher#isCoordinator()
	 */
	public boolean isCoordinator()
	{
		return this.getLocalAddress().equals(this.getCoordinatorAddress());
	}
	
	/**
	 * @see net.sf.hajdbc.distributable.jgroups.CommandDispatcher#getLocal()
	 */
	public Member getLocal()
	{
		return new AddressMember(this.getLocalAddress());
	}
	
	private Address getLocalAddress()
	{
		return this.dispatcher.getChannel().getAddress();
	}
	
	private Address getCoordinatorAddress()
	{
		return this.dispatcher.getChannel().getView().getMembers().get(0);
	}

	/**
	 * {@inheritDoc}
	 * @see org.jgroups.blocks.RequestHandler#handle(org.jgroups.Message)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Object handle(Message message)
	{
		if (!(message instanceof Command)) return null;
		
		Command<Object, C> command = (Command) message.getObject();

		this.logger.log(Level.DEBUG, Messages.COMMAND_RECEIVED.getMessage(command));
		
		return command.marshalResult(command.execute(this.context));
	}

	/**
	 * {@inheritDoc}
	 * @see org.jgroups.MembershipListener#viewAccepted(org.jgroups.View)
	 */
	@Override
	public void viewAccepted(View view)
	{
		if (this.membershipListener != null)
		{
			View oldView = this.viewReference.getAndSet(view);
			
			for (Address address: view.getMembers())
			{
				if ((oldView == null) || !oldView.containsMember(address))
				{
					this.membershipListener.added(new AddressMember(address));
				}
			}
			
			if (oldView != null)
			{
				for (Address address: oldView.getMembers())
				{
					if (!view.containsMember(address))
					{
						this.membershipListener.removed(new AddressMember(address));
					}
				}
			}
		}
	}

	/**
	 * {@inheritDoc}
	 * @see org.jgroups.MembershipListener#block()
	 */
	@Override
	public void block()
	{
	}

	/**
	 * {@inheritDoc}
	 * @see org.jgroups.ExtendedMembershipListener#unblock()
	 */
	@Override
	public void unblock()
	{
	}

	/**
	 * {@inheritDoc}
	 * @see org.jgroups.MembershipListener#suspect(org.jgroups.Address)
	 */
	@Override
	public void suspect(Address address)
	{
	}

	/**
	 * {@inheritDoc}
	 * @see org.jgroups.MessageListener#getState()
	 */
	@Override
	public byte[] getState()
	{
		return this.stateful.getState();
	}

	/**
	 * {@inheritDoc}
	 * @see org.jgroups.MessageListener#receive(org.jgroups.Message)
	 */
	@Override
	public void receive(Message msg)
	{
	}

	/**
	 * {@inheritDoc}
	 * @see org.jgroups.MessageListener#setState(byte[])
	 */
	@Override
	public void setState(byte[] state)
	{
		this.stateful.setState(state);
	}
}
