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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import org.jgroups.Address;

/**
 * @author Paul Ferraro
 * @since 2.0
 */
public abstract class AbstractLockDecree implements LockDecree, Externalizable
{
	private String id;
	private Address address;
	
	protected AbstractLockDecree(String id, Address address)
	{
		this.id = id;
	}

	protected AbstractLockDecree()
	{
		// Required for deserialization
	}

	/**
	 * @see net.sf.hajdbc.distributable.LockDecree#getId()
	 */
	public String getId()
	{
		return this.id;
	}
	
	/**
	 * @see net.sf.hajdbc.distributable.LockDecree#getAddress()
	 */
	public Address getAddress()
	{
		return this.address;
	}
	
	protected void unlock(Map<LockDecree, Lock> lockMap)
	{
		synchronized (lockMap)
		{
			Lock lock = lockMap.remove(this);
			
			if (lock != null)
			{
				lock.unlock();
			}
		}
	}
	
	/**
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object)
	{
		if ((object == null) || !(object instanceof LockDecree)) return false;
		
		String id = ((LockDecree) object).getId();
		
		return (id != null) && id.equals(this.id);
	}

	/**
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		return this.id.hashCode();
	}

	/**
	 * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
	 */
	@Override
	public void writeExternal(ObjectOutput output) throws IOException
	{
		output.writeUTF(this.id);
		output.writeObject(this.address);
	}

	/**
	 * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
	 */
	@Override
	public void readExternal(ObjectInput input) throws IOException, ClassNotFoundException
	{
		this.id = input.readUTF();
		this.address = (Address) input.readObject();
	}
}