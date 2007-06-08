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

/**
 * Represents a database command to be executed on a given database cluster.
 * @author  Paul Ferraro
 * @since   1.0
 */
public abstract class AbstractCommand implements Command<Boolean>, Externalizable
{
	protected String databaseId;
	
	/**
	 * Constructs a new AbstractDatabaseCommand.
	 */
	protected AbstractCommand()
	{
		// Do nothing
	}
	
	/**
	 * Constructs a new AbstractDatabaseCommand.
	 * @param databaseId a database identifier
	 */
	public AbstractCommand(String databaseId)
	{
		this.databaseId = databaseId;
	}
	
	/**
	 * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
	 */
	@Override
	public void writeExternal(ObjectOutput output) throws IOException
	{
		output.writeUTF(this.databaseId);
	}

	/**
	 * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
	 */
	@Override
	public void readExternal(ObjectInput input) throws IOException
	{
		this.databaseId = input.readUTF();
	}
	
	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return this.getClass().getName() + " [" + this.databaseId + "]";
	}

	/**
	 * @see net.sf.hajdbc.distributable.Command#marshalResult(java.lang.Object)
	 */
	@Override
	public Object marshalResult(Boolean result)
	{
		return result;
	}

	/**
	 * @see net.sf.hajdbc.distributable.Command#unmarshalResult(java.io.Serializable)
	 */
	@Override
	public Boolean unmarshalResult(Object result)
	{
		return Boolean.class.cast(result);
	}
}
