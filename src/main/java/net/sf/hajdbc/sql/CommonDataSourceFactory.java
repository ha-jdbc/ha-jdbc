package net.sf.hajdbc.sql;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.RefAddr;
import javax.naming.Reference;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseClusterConfigurationFactory;
import net.sf.hajdbc.util.Objects;

public class CommonDataSourceFactory<Z extends javax.sql.CommonDataSource, D extends Database<Z>, F extends CommonDataSourceProxyFactory<Z, D>> implements javax.naming.spi.ObjectFactory
{
	public static final String CLUSTER = "cluster";
	public static final String CONFIG = "config";
	public static final String USER = "user";
	public static final String PASSWORD = "password";
	
	@Override
	public Object getObjectInstance(Object object, Name name, Context nameCtx, Hashtable<?, ?> environment) throws Exception
	{
		if ((object == null) || !(object instanceof Reference)) return null;
		
		Reference reference = (Reference) object;
		
		String className = reference.getClassName();
		
		if (className == null) return null;

		Class<?> targetClass = this.getClass().getClassLoader().loadClass(className);
		
		if (!targetClass.isAssignableFrom(CommonDataSource.class)) return null;
		
		@SuppressWarnings("unchecked")
		CommonDataSource<Z, D, F> result = targetClass.asSubclass(CommonDataSource.class).newInstance();
		
		RefAddr clusterAddr = reference.get(CLUSTER);
		
		if (clusterAddr == null) return null;

		Object clusterAddrContent = clusterAddr.getContent();
		
		if (!(clusterAddrContent instanceof String)) return null;

		result.setCluster((String) clusterAddrContent);
		
		RefAddr configAddr = reference.get(CONFIG);
		
		if (configAddr == null) return null;

		Object configAddrContent = configAddr.getContent();
		
		if (configAddrContent == null) return null;
		
		if (configAddrContent instanceof String)
		{
			result.setConfig((String) configAddrContent);
		}
		else if (configAddrContent instanceof byte[])
		{
			byte[] config = (byte[]) configAddrContent;
			
			result.setConfigurationFactory(Objects.<DatabaseClusterConfigurationFactory<Z, D>>deserialize(config));
		}
		
		RefAddr userAddr = reference.get(USER);
		if (userAddr != null)
		{
			Object userAddrContent = userAddr.getContent();
			if (userAddrContent instanceof String)
			{
				result.setUser((String) userAddrContent);
			}
		}
		
		RefAddr passwordAddr = reference.get(USER);
		if (passwordAddr != null)
		{
			Object passwordAddrContent = passwordAddr.getContent();
			if (passwordAddrContent instanceof String)
			{
				result.setPassword((String) passwordAddrContent);
			}
		}

		return result;
	}
}
