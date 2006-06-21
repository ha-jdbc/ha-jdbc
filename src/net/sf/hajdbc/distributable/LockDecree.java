/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2006 Paul Ferraro
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
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.LockManager;

/**
 * @author Paul Ferraro
 * @since 1.2
 */
public abstract class LockDecree implements Externalizable
{
	private String id;
	
	protected LockDecree(String id)
	{
		this.id = id;
	}

	protected LockDecree()
	{
		// Required for deserialization
	}

	/**
	 * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
	 */
	public void writeExternal(ObjectOutput output) throws IOException
	{
		output.writeUTF(this.id);
	}

	/**
	 * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
	 */
	public void readExternal(ObjectInput input) throws IOException
	{
		this.id = input.readUTF();
	}
	
	/**
	 * Prepare phase of 2-phase vote
	 * @param lockManager
	 * @return
	 */
	public abstract boolean prepare(LockManager lockManager);
	
	/**
	 * Commit phase of 2-phase vote
	 * @param lockManager
	 * @return
	 */
	public abstract boolean commit(LockManager lockManager);
	
	/**
	 * Called when prepare phase fails.
	 * @param lockManager
	 */
	public abstract void abort(LockManager lockManager);
	
	protected Lock getLock(LockManager lockManager)
	{
		return lockManager.writeLock(this.id);
	}
}