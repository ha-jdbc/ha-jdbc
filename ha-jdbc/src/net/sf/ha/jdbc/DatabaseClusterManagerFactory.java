package net.sf.ha.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jibx.runtime.BindingDirectory;
import org.jibx.runtime.IBindingFactory;
import org.jibx.runtime.IUnmarshallingContext;
import org.jibx.runtime.JiBXException;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class DatabaseClusterManagerFactory
{
	private static Log log = LogFactory.getLog(DatabaseClusterManagerFactory.class);
	private static DatabaseClusterManager databaseClusterManager = null;
	
	public static synchronized DatabaseClusterManager getClusterManager() throws SQLException
	{
		if (databaseClusterManager == null)
		{
			URL url = Thread.currentThread().getContextClassLoader().getResource("ha-jdbc.xml");
			InputStream inputStream = null;
			
			try
			{
				inputStream = url.openStream();
				
				IBindingFactory factory = BindingDirectory.getFactory(DatabaseClusterManager.class);
				IUnmarshallingContext context = factory.createUnmarshallingContext();
				
				databaseClusterManager = (DatabaseClusterManager) context.unmarshalDocument(new InputStreamReader(inputStream));
			}
			catch (IOException e)
			{
				SQLException exception = new SQLException("Failed to read " + url);
				exception.initCause(e);
				throw exception;
			}
			catch (JiBXException e)
			{
				SQLException exception = new SQLException("Failed to parse " + url);
				exception.initCause(e);
				throw exception;
			}
			finally
			{
				if (inputStream != null)
				{
					try
					{
						inputStream.close();
					}
					catch (IOException e)
					{
						log.warn("Failed to close " + url, e);
					}
				}
			}
		}
		
		return databaseClusterManager;
	}
}
