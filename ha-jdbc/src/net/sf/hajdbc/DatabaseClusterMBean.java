/*
 * Copyright (c) 2004, Identity Theft 911, LLC.  All rights reserved.
 *
 * $Id$
 */
package net.sf.hajdbc;

import java.sql.SQLException;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public interface DatabaseClusterMBean
{
	public String getName();
	
	public boolean isActive(String databaseId);
	
	public void deactivate(String databaseId);
	
	public void activate(String databaseId, String strategyClassName) throws SQLException;
}
