/*
 * Copyright (c) 2004-2007, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc.sql;

import java.sql.SQLException;
import java.util.HashMap;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;

/**
 * @author Paul Ferraro
 */
public class AbstractRootInvocationHandler<D> extends AbstractInvocationHandler<D, D>
{
	/**
	 * Constructs a new AbstractRootInvocationHandler.
	 * @param databaseCluster
	 * @param proxyClass
	 * @param objectMap
	 */
	protected AbstractRootInvocationHandler(DatabaseCluster<D> databaseCluster, Class<D> proxyClass)
	{
		super(databaseCluster, proxyClass, new HashMap<Database<D>, D>());
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#close(net.sf.hajdbc.Database, java.lang.Object)
	 */
	@Override
	protected void close(Database<D> database, D object)
	{
		// Nothing to close
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#createObject(net.sf.hajdbc.Database)
	 */
	@Override
	protected D createObject(Database<D> database) throws SQLException
	{
		return database.createConnectionFactory();
	}

	/**
	 * @see net.sf.hajdbc.sql.SQLProxy#getRoot()
	 */
	@Override
	public SQLProxy<D, ?> getRoot()
	{
		return this;
	}
}
