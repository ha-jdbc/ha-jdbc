/*
 * Copyright (c) 2004-2006, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc.sql;

import net.sf.hajdbc.ActiveDatabaseMBean;

/**
 * @author Paul Ferraro
 *
 */
public interface ActiveDriverDatabaseMBean extends ActiveDatabaseMBean
{
	/**
	 * Returns the url for this database
	 * @return a database url
	 */
	public String getUrl();
	
	/**
	 * Returns the driver class name for this database.
	 * @return a driver class name
	 */
	public String getDriver();
}
