/*
 * Copyright (c) 2004, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.ha.jdbc;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
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

	public String getName()
	{
		return this.databaseCluster.getDescriptor().getName();
	}
	
	public void setName(String name) throws SQLException, NamingException
	{
		DatabaseClusterManager manager = DatabaseClusterManagerFactory.getClusterManager();
		DatabaseClusterDescriptor descriptor = manager.getDescriptor(name);
		List databaseList = descriptor.getActiveDatabaseList();
		Map dataSourceMap = new HashMap(databaseList.size());
		Context context = new InitialContext();
		
		for (int i = 0; i < databaseList.size(); ++i)
		{
			DataSourceDatabase database = (DataSourceDatabase) databaseList.get(i);
			Object object = context.lookup(database.getName());
			
			dataSourceMap.put(database, object);
		}
		
		this.databaseCluster = new DatabaseCluster(manager, descriptor, dataSourceMap);
	}
	
	/**
	 * @see javax.naming.Referenceable#getReference()
	 */
	public final Reference getReference()
	{
        Reference ref = new Reference(this.getClass().getName(), this.getObjectFactoryClass().getName(), null);
        
        ref.add(new StringRefAddr(AbstractDataSourceFactory.CLUSTER_NAME, this.getName()));
        
        return ref;
	}
	
	protected abstract Class getObjectFactoryClass();
}
