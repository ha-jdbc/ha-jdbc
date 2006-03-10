/*
 * Copyright (c) 2004-2006, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc.sql;

import net.sf.hajdbc.ActiveDatabaseMBean;

/**
 * @author Paul Ferraro
 *
 */
public interface ActiveDataSourceDatabaseMBean extends ActiveDatabaseMBean
{
	/**
	 * Return the JNDI name of this DataSource
	 * @return a JNDI name
	 */
	public String getName();
}
