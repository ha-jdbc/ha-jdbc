/*
 * Copyright (c) 2004, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc.distributable;

import net.sf.hajdbc.DatabaseClusterListener;
import net.sf.hajdbc.DatabaseClusterListenerDescriptor;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class DistributableDatabaseClusterListenerDescriptor implements DatabaseClusterListenerDescriptor
{
	private String protocol;
	
	public String getProtocol()
	{
		return this.protocol;
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterListenerDescriptor#getListener(java.lang.String)
	 */
	public DatabaseClusterListener getListener(String name)
	{
		return null;
	}
}
