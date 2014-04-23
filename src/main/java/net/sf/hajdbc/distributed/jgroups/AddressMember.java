/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.sf.hajdbc.distributed.jgroups;

import net.sf.hajdbc.distributed.Member;

import org.jgroups.Address;

/**
 * Identifies a group member using the JGroups Address.
 * 
 * @author Paul Ferraro
 * @see org.jgroups.Address
 */
public class AddressMember implements Member
{
	private static final long serialVersionUID = -5777399287019796606L;
	
	private final Address address;
	
	/**
	 * Constructs a new AddressMember
	 * @param address the member's address
	 */
	public AddressMember(Address address)
	{
		this.address = address;
	}

	public Address getAddress()
	{
		return this.address;
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object)
	{
		if ((object == null) || !(object instanceof AddressMember)) return false;

		AddressMember member = (AddressMember) object;
		
		return this.address.equals(member.address);
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		return this.address.hashCode();
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return this.address.toString();
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(Member member)
	{
		return this.address.compareTo(((AddressMember) member).address);
	}
}
