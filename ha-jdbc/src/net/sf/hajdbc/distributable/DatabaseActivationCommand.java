/*
 * Copyright (c) 2004, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc.distributable;


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
	 * @see org.jgroups.util.Command#execute()
	 */
	public boolean execute()
	{
		return false;
	}
}
