/*
 * Copyright (c) 2004, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc.distributable;

import java.sql.SQLException;

import net.sf.hajdbc.DatabaseClusterMBean;


/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class DatabaseActivationCommand extends DatabaseCommand
{
	/**
	 * Constructs a new DatabaseActivationCommand.
	 * @param databaseId
	 */
	public DatabaseActivationCommand(String databaseId)
	{
		super(databaseId);
	}

	/**
	 * @see net.sf.hajdbc.distributable.DatabaseCommand#execute(net.sf.hajdbc.DatabaseCluster)
	 */
	public void execute(DatabaseClusterMBean databaseCluster) throws SQLException
	{
		databaseCluster.activate(this.databaseId, null);
	}
}
