/*
 * Copyright (c) 2004, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.ha.jdbc;

import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public abstract class AbstractDataSourceProxy implements Referenceable
{
	protected DatabaseCluster databaseCluster;

	public DatabaseCluster getDatabaseCluster()
	{
		return this.databaseCluster;
	}
	
	public void setDatabaseCluster(DatabaseCluster databaseCluster)
	{
		this.databaseCluster = databaseCluster;
	}

	/**
	 * @see javax.naming.Referenceable#getReference()
	 */
	public final Reference getReference()
	{
        Reference ref = new Reference(this.getClass().getName(), this.getObjectFactoryClass().getName(), null);
        
        ref.add(new StringRefAddr(AbstractDataSourceFactory.CLUSTER_NAME, this.databaseCluster.getDescriptor().getName()));
        
        return ref;
	}
	
	protected abstract Class getObjectFactoryClass();
}
