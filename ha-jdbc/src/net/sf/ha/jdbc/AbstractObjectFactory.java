package net.sf.ha.jdbc;

import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public abstract class AbstractObjectFactory implements ObjectFactory
{
	public static final String CLUSTER_NAME = "name";
	
	/**
	 * @see javax.naming.spi.ObjectFactory#getObjectInstance(java.lang.Object, javax.naming.Name, javax.naming.Context, java.util.Hashtable)
	 */
	public Object getObjectInstance(Object refObject, Name name, Context context, Hashtable environment) throws Exception
	{
		Reference reference = (Reference) refObject;
		
		if (refObject == null)
		{
			return null;
		}
		
		String className = reference.getClassName();
		
		if (className == null)
		{
			return null;
		}
		
		Class objectClass = this.getObjectClass();
		
		if (!objectClass.getName().equals(className))
		{
			return null;
		}
		
		AbstractDataSourceProxy dataSource = (AbstractDataSourceProxy) objectClass.newInstance();
		
		String clusterName = (String) reference.get(CLUSTER_NAME).getContent();
		Set databaseSet = DatabaseClusterManagerFactory.getClusterManager().getDatabaseSet(clusterName);
		Map dataSourceMap = new HashMap(databaseSet.size());
		Iterator databases = databaseSet.iterator();
		
		while (databases.hasNext())
		{
			DataSourceDatabase database = (DataSourceDatabase) databases.next();
			Object object = context.lookup(database.getName());
			
			dataSourceMap.put(database, object);
		}
		
		DatabaseCluster databaseCluster = new DatabaseCluster(Collections.unmodifiableMap(dataSourceMap));
		databaseCluster.setName(clusterName);
		databaseCluster.setValidateSQL("SELECT 1");
		
		dataSource.setDatabaseManager(databaseCluster);
		
		return dataSource;
	}
	
	protected abstract Class getObjectClass();
}
