/*
 * Copyright (c) 2004, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc;

import java.sql.SQLException;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public interface DatabaseSynchronizationStrategy
{
	public void synchronize(DatabaseCluster databaseCluster, Database database) throws SQLException;
}
