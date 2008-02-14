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

	protected AbstractMembershipListener(Channel channel)
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

	public int getMembershipSize()
	{
		synchronized (this.addressSet)
		{
			return this.addressSet.size();
		}
	}
	
	public boolean isMembershipEmpty()
	{
		synchronized (this.addressSet)
		{
			return this.addressSet.isEmpty();
		}
	}

	protected abstract void memberJoined(Address address);
	
	protected abstract void memberLeft(Address address);
}
