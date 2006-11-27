/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2006 Paul Ferraro
 * 
 * This library is free software; you can redistribute it and/or modify it 
 * under the terms of the GNU Lesser General Public License as published by the 
 * Free Software Foundation; either version 2.1 of the License, or (at your 
 * option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License 
 * for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation, 
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 * 
 * Contact: ferraro@users.sourceforge.net
 */
package net.sf.hajdbc.dialect;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.TableProperties;

/**
 * Dialect for <a href="http://postgresql.org">PostgreSQL</a>.
 * @author  Paul Ferraro
 * @since   1.1
 */
public class PostgreSQLDialect extends StandardDialect
{
	/**
	 * PostgreSQL uses a schema search path to locate unqualified table names.
	 * The default search path is [$user,public], where $user is the current user.
	 * @see net.sf.hajdbc.dialect.StandardDialect#getDefaultSchemas(java.sql.Connection)
	 */
	@Override
	public List<String> getDefaultSchemas(Connection connection) throws SQLException
	{
		Statement statement = connection.createStatement();
		
		ResultSet resultSet = statement.executeQuery("SHOW search_path");
		
		resultSet.next();
		
		String[] schemas = resultSet.getString(1).split(",");
		
		resultSet.close();
		statement.close();
		
		List<String> schemaList = new ArrayList<String>(schemas.length);
		
		for (String schema: schemas)
		{
			schemaList.add(schema.equals("$user") ? super.getCurrentUser(connection) : schema);
		}
		
		return schemaList;
	}

	/**
	 * Default implementation does not block INSERT statements in PostgreSQL.
	 * Requires explicit exclusive mode table lock.
	 * <p><em>From PostgreSQL documentation</em></p>
	 * Unlike traditional database systems which use locks for concurrency control, PostgreSQL maintains data consistency by using a multiversion model (Multiversion Concurrency Control, MVCC).
	 * This means that while querying a database each transaction sees a snapshot of data (a database version) as it was some time ago, regardless of the current state of the underlying data.
	 * This protects the transaction from viewing inconsistent data that could be caused by (other) concurrent transaction updates on the same data rows, providing transaction isolation for each database session.	 * 
	 * @see net.sf.hajdbc.dialect.StandardDialect#getLockTableSQL(net.sf.hajdbc.TableProperties)
	 */
	@Override
	public String getLockTableSQL(TableProperties properties)
	{
		return MessageFormat.format("LOCK TABLE {0} IN EXCLUSIVE MODE; SELECT 1 FROM {0}", properties.getName());
	}
	
	/**
	 * PostgreSQL uses the native type OID for BLOBs.
	 * However the driver maps OID to INTEGER.
	 * OID columns should really be mapped to BLOB.
	 * @see net.sf.hajdbc.dialect.StandardDialect#getColumnType(net.sf.hajdbc.ColumnProperties)
	 */
	@Override
	public int getColumnType(ColumnProperties properties)
	{
		return properties.getNativeType().equalsIgnoreCase("oid") ? Types.BLOB : properties.getType();
	}
	
	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#isIdentity(net.sf.hajdbc.ColumnProperties)
	 */
	@Override
	public boolean isIdentity(ColumnProperties properties)
	{
		String type = properties.getNativeType();
		
		return type.equalsIgnoreCase("serial") || type.equalsIgnoreCase("bigserial");
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#truncateTableFormat()
	 */
	@Override
	protected String truncateTableFormat()
	{
		return "TRUNCATE TABLE {0}";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#sequencePattern()
	 */
	@Override
	protected String sequencePattern()
	{
		return "(?:(?:CURR)|(?:NEXT))VAL\\s*\\(\\s*'(\\S+)'\\s*\\)";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#currentSequenceValueFormat()
	 */
	@Override
	protected String nextValueForSequenceFormat()
	{
		return "NEXTVAL(''{0}'')";
	}
}
