package net.sf.ha.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.sql.DataSource;

/**
 * @author Paul Ferraro
 * @version $Revision$
 */
public class DataSourceProxy extends DatabaseManager implements DataSource, Referenceable
{
	public DataSourceProxy(Map dataSourceMap)
	{
		super(dataSourceMap);
	}
	
	/**
	 * @see javax.sql.DataSource#getLoginTimeout()
	 */
	public int getLoginTimeout() throws SQLException
	{
		DataSourceOperation operation = new DataSourceOperation()
		{
			public Object execute(Database database, DataSource dataSource) throws SQLException
			{
				return new Integer(dataSource.getLoginTimeout());
			}
		};
		
		return ((Integer) this.executeRead(operation)).intValue();
	}

	/**
	 * @see javax.sql.DataSource#setLoginTimeout(int)
	 */
	public void setLoginTimeout(final int seconds) throws SQLException
	{
		DataSourceOperation operation = new DataSourceOperation()
		{
			public Object execute(Database database, DataSource dataSource) throws SQLException
			{
				dataSource.setLoginTimeout(seconds);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.DataSource#getLogWriter()
	 */
	public PrintWriter getLogWriter() throws SQLException
	{
		DataSourceOperation operation = new DataSourceOperation()
		{
			public Object execute(Database database, DataSource dataSource) throws SQLException
			{
				return dataSource.getLogWriter();
			}
		};
		
		return (PrintWriter) this.executeRead(operation);
	}

	/**
	 * @see javax.sql.DataSource#setLogWriter(java.io.PrintWriter)
	 */
	public void setLogWriter(final PrintWriter writer) throws SQLException
	{
		DataSourceOperation operation = new DataSourceOperation()
		{
			public Object execute(Database database, DataSource dataSource) throws SQLException
			{
				dataSource.setLogWriter(writer);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.DataSource#getConnection()
	 */
	public Connection getConnection() throws SQLException
	{
		DataSourceOperation operation = new DataSourceOperation()
		{
			public Object execute(Database database, DataSource dataSource) throws SQLException
			{
				return dataSource.getConnection();
			}
		};
		
		return new ConnectionProxy(this, this.executeWrite(operation));
	}

	/**
	 * @see javax.sql.DataSource#getConnection(java.lang.String, java.lang.String)
	 */
	public Connection getConnection(final String user, final String password) throws SQLException
	{
		DataSourceOperation operation = new DataSourceOperation()
		{
			public Object execute(Database database, DataSource dataSource) throws SQLException
			{
				return dataSource.getConnection(user, password);
			}
		};
		
		return new ConnectionProxy(this, this.executeWrite(operation));
	}

	/**
	 * @see javax.naming.Referenceable#getReference()
	 */
	public Reference getReference() throws NamingException
	{
        Reference ref = new Reference(this.getClass().getName(), DataSourceFactory.class.getName(), null);
/*        
        ref.add(new StringRefAddr("user", getUser()));
        ref.add(new StringRefAddr("password", password));
        ref.add(new StringRefAddr("serverName", getServerName()));
        ref.add(new StringRefAddr("port", "" + getPort()));
        ref.add(new StringRefAddr("databaseName", getDatabaseName()));
        ref.add(new StringRefAddr("profileSql", getProfileSql()));
        ref.add(new StringRefAddr("explicitUrl", String.valueOf(this.explicitUrl)));
        ref.add(new StringRefAddr("url", getUrl()));
*/
        if (false)
        {
        	throw new NamingException();
        }
        
        return ref;
	}
	
	protected Connection connect(Database database, Object object) throws SQLException
	{
		DataSourceDatabase info = (DataSourceDatabase) database;
		DataSource dataSource = (DataSource) object;
		String user = info.getUser();
		
		return (user == null) ? dataSource.getConnection() : dataSource.getConnection(user, info.getPassword());
	}
}
