package net.sf.ha.jdbc;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

import net.sf.ha.jdbc.pool.ConnectionPoolDataSourceProxy;
import net.sf.ha.jdbc.pool.xa.XADataSourceProxy;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 */
public class DataSourceFactory implements ObjectFactory
{
	public static final Set DATA_SOURCE_SET = buildDataSourceSet();
	
	private static Set buildDataSourceSet()
	{
		Set set = new HashSet(3);
		
		set.add(DataSourceProxy.class.getName());
		set.add(ConnectionPoolDataSourceProxy.class.getName());
		set.add(XADataSourceProxy.class.getName());
		
		return set;
	}
	
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
		
		if (!DATA_SOURCE_SET.contains(className))
		{
			return null;
		}
		
		DataSourceProxy dataSource = (DataSourceProxy) Class.forName(className).newInstance();
		
		return dataSource;
	}
}
