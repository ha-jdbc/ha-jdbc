/*
 * Copyright (c) 2004-2007, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc.distributable;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.MembershipListener;
import org.jgroups.View;

/**
 * @author Paul Ferraro
 */
public abstract class AbstractMembershipListener implements MembershipListener
{
	protected Channel channel;
	private Set<Address> addressSet = new HashSet<Address>();

	public AbstractMembershipListener(Channel channel)
	{
		this.channel = channel;
		
		this.channel.setOpt(Channel.LOCAL, false);
	}
	
	/**
	 * @see org.jgroups.MembershipListener#block()
	 */
	@Override
	public final void block()
	{
		// Do nothing
	}

	/**
	 * @see org.jgroups.MembershipListener#suspect(org.jgroups.Address)
	 */
	@Override
	public final void suspect(Address address)
	{
		// Do nothing
	}
	
	/**
	 * @see org.jgroups.MembershipListener#viewAccepted(org.jgroups.View)
	 */
	@Override
	public final void viewAccepted(View view)
	{
		synchronized (this.addressSet)
		{
			Iterator<Address> addresses = this.addressSet.iterator();
			
			while (addresses.hasNext())
			{
				Address address = addresses.next();
				
				if (!view.containsMember(address))
				{
					addresses.remove();
					
					this.memberLeft(address);
				}
			}
			
			Address localAddress = this.channel.getLocalAddress();
			
			for (Address address: view.getMembers())
			{
				// Track remote members only
				if (!address.equals(localAddress) && this.addressSet.add(address))
				{
					this.memberJoined(address);
				}
			}
		}
	}

	protected boolean hasNoMembers()
	{
		synchronized (this.addressSet)
		{
			return this.addressSet.isEmpty();
		}
	}

	protected abstract void memberJoined(Address address);
	
	protected abstract void memberLeft(Address address);
}
