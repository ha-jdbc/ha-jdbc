/*
 * Copyright (c) 2004, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc.distributable;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;


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
	public DatabaseActivationCommand(Database database)
	{
		super(database);
	}

	/**
	 * @see net.sf.hajdbc.distributable.DatabaseCommand#execute(net.sf.hajdbc.DatabaseCluster)
	 */
	public void execute(DatabaseCluster databaseCluster)
	{
		databaseCluster.activate(this.databaseId);
	}
}
