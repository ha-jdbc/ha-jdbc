/*
 * HA-JDBC: High-Availablity JDBC
 * Copyright 2010 Paul Ferraro
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.sf.hajdbc;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.SQLException;
import java.util.Hashtable;
import java.util.Map;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * @author Paul Ferraro
 */
public class ContextDatabaseClusterConfigurationFactory<Z, D extends Database<Z>> implements DatabaseClusterConfigurationFactory<Z, D>
{
	private static final long serialVersionUID = -914797510468986091L;

	private final Class<? extends DatabaseClusterConfiguration<Z, D>> targetClass;
	private final String name;
	private transient Context context;
	
	public ContextDatabaseClusterConfigurationFactory(Class<? extends DatabaseClusterConfiguration<Z, D>> targetClass, String name) throws NamingException
	{
		this(targetClass, name, new InitialContext());
	}
	
	public ContextDatabaseClusterConfigurationFactory(Class<? extends DatabaseClusterConfiguration<Z, D>> targetClass, String name, Hashtable<?, ?> env) throws NamingException
	{
		this(targetClass, name, new InitialContext(env));
	}
	
	public ContextDatabaseClusterConfigurationFactory(Class<? extends DatabaseClusterConfiguration<Z, D>> targetClass, String name, Context context)
	{
		this.targetClass = targetClass;
		this.name = name;
		this.context = context;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseClusterConfigurationFactory#createConfiguration()
	 */
	@Override
	public DatabaseClusterConfiguration<Z, D> createConfiguration() throws SQLException
	{
		try
		{
			return this.targetClass.cast(this.context.lookup(this.name));
		}
		catch (NamingException e)
		{
			throw new SQLException(e);
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseClusterConfigurationListener#added(net.sf.hajdbc.Database, net.sf.hajdbc.DatabaseClusterConfiguration)
	 */
	@Override
	public void added(D database, DatabaseClusterConfiguration<Z, D> configuration)
	{
		this.rebind(configuration);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseClusterConfigurationListener#removed(net.sf.hajdbc.Database, net.sf.hajdbc.DatabaseClusterConfiguration)
	 */
	@Override
	public void removed(D database, DatabaseClusterConfiguration<Z, D> configuration)
	{
		this.rebind(configuration);
	}

	private void rebind(DatabaseClusterConfiguration<Z, D> configuration)
	{
		try
		{
			this.context.rebind(this.name, configuration);
		}
		catch (NamingException e)
		{
			
		}
	}
	
	private void writeObject(ObjectOutputStream output) throws IOException
	{
		output.defaultWriteObject();

		try
		{
			Hashtable<?, ?> env = this.context.getEnvironment();
			
			output.writeInt((env != null) ? env.size() : 0);
			
			for (Map.Entry<?, ?> entry: env.entrySet())
			{
				output.writeUTF((String) entry.getKey());
				output.writeUTF((String) entry.getValue());
			}
		}
		catch (NamingException e)
		{
			throw new IOException(e);
		}
	}
	
	private void readObject(ObjectInputStream input) throws IOException, ClassNotFoundException
	{
		input.defaultReadObject();
		
		int size = input.readInt();
		
		Hashtable<String, String> env = (size > 0) ? new Hashtable<String, String>() : null;
		
		for (int i = 0; i < input.readInt(); ++i)
		{
			env.put(input.readUTF(), input.readUTF());
		}
		
		try
		{
			this.context = new InitialContext(env);
		}
		catch (NamingException e)
		{
			throw new IOException(e);
		}
	}
}
