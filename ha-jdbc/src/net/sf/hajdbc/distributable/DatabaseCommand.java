/*
 * Copyright (c) 2004, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc.distributable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.SQLException;

import net.sf.hajdbc.DatabaseClusterMBean;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public abstract class DatabaseCommand implements Externalizable
{
	protected String databaseId;
	
	/**
	 * Constructs a new DatabaseCommand.
	 * @param databaseId
	 */
	public DatabaseCommand(String databaseId)
	{
		this.databaseId = databaseId;
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
	
	public abstract void execute(DatabaseClusterMBean databaseCluster) throws SQLException;
}
