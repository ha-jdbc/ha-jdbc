/*
 * Copyright (c) 2004, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc;

import java.util.Map;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class DatabaseConnector extends SQLProxy
{
	private DatabaseCluster databaseCluster;
	
	public DatabaseConnector(DatabaseCluster databaseCluster, Map databaseConnectorMap)
	{
		super(databaseConnectorMap);
		
		this.databaseCluster = databaseCluster;
	}
	
	/**
	 * @see net.sf.hajdbc.SQLProxy#getDatabaseCluster()
	 */
	protected DatabaseCluster getDatabaseCluster()
	{
		return this.databaseCluster;
	}
}
