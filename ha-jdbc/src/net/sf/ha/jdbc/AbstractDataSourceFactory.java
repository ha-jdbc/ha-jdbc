package net.sf.ha.jdbc;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public abstract class AbstractDataSourceFactory implements ObjectFactory
{
	public static final String CLUSTER_NAME = "name";
	
	/**
	 * @see javax.naming.spi.ObjectFactory#getObjectInstance(java.lang.Object, javax.naming.Name, javax.naming.Context, java.util.Hashtable)
	 */
	public Object getObjectInstance(Object object, Name name, Context context, Hashtable environment) throws Exception
	{
		if (object == null)
		{
			return null;
		}
		
		if (!Reference.class.isInstance(object))
		{
			return null;
		}
		
		Reference reference = (Reference) object;
		
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
		
		dataSource.setName(clusterName);
		
		return dataSource;
	}
	
	protected abstract Class getObjectClass();
}
