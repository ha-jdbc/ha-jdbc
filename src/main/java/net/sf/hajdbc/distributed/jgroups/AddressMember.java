package net.sf.hajdbc.distributed.jgroups;

import net.sf.hajdbc.distributed.Member;

import org.jgroups.Address;

public class AddressMember implements Member
{
	private static final long serialVersionUID = -5777399287019796606L;
	
	private final Address address;
	
	public AddressMember(Address address)
	{
		this.address = address;
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

	@Override
	public int compareTo(Member member)
	{
		return this.address.compareTo(((AddressMember) member).address);
	}
}
