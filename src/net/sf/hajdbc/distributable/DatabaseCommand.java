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

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;

/**
 * Represents a database command to be executed on a given database cluster.
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public abstract class DatabaseCommand implements Externalizable
{
	protected String databaseId;
	
	/**
	 * Constructs a new DatabaseCommand.
	 */
	protected DatabaseCommand()
	{
		// Do nothing
	}
	
	/**
	 * Constructs a new DatabaseCommand.
	 * @param database a database descriptor
	 */
	public DatabaseCommand(Database database)
	{
		this.databaseId = database.getId();
	}
	
	/**
	 * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
	 */
	public void writeExternal(ObjectOutput output) throws IOException
	{
		output.writeUTF(this.databaseId);
	}

	/**
	 * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
	 */
	public void readExternal(ObjectInput input) throws IOException
	{
		this.databaseId = input.readUTF();
	}
	
	/**
	 * Execute this command on the specified database cluster.
	 * @param databaseCluster a database cluster
	 * @throws java.sql.SQLException if command failed to execute
	 */
	public abstract void execute(DatabaseCluster databaseCluster) throws java.sql.SQLException;
}
