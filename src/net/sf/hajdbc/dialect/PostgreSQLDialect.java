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
import java.sql.Types;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.TableProperties;
import net.sf.hajdbc.util.Strings;

/**
 * Dialect for <a href="http://postgresql.org">PostgreSQL</a>.
 * @author  Paul Ferraro
 * @since   1.1
 */
public class PostgreSQLDialect extends DefaultDialect
{
	/**
	 * Default implementation does not block INSERT statements.
	 * Requires explicit exclusive mode table lock.
	 * <p><em>From PostgreSQL documentation</em></p>
	 * Unlike traditional database systems which use locks for concurrency control, PostgreSQL maintains data consistency by using a multiversion model (Multiversion Concurrency Control, MVCC).
	 * This means that while querying a database each transaction sees a snapshot of data (a database version) as it was some time ago, regardless of the current state of the underlying data.
	 * This protects the transaction from viewing inconsistent data that could be caused by (other) concurrent transaction updates on the same data rows, providing transaction isolation for each database session.	 * 
	 * @see net.sf.hajdbc.dialect.DefaultDialect#getLockTableSQL(java.sql.DatabaseMetaData, java.lang.String, java.lang.String)
	 */
	@Override
	public String getLockTableSQL(TableProperties properties) throws SQLException
	{
		return MessageFormat.format("LOCK TABLE {0} IN EXCLUSIVE MODE; SELECT 1 FROM {0}", properties.getName());
	}
	
	/**
	 * PostgreSQL uses the native type OID for BLOBs.  However the driver maps OID to INTEGER.  OID columns should really be mapped to BLOB.
	 * @see net.sf.hajdbc.dialect.DefaultDialect#getColumnType(net.sf.hajdbc.DatabaseMetaDataCache, java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public int getColumnType(ColumnProperties properties)
	{
		return properties.getNativeType().equals("oid") ? Types.BLOB : properties.getType();
	}

	/**
	 * @see net.sf.hajdbc.Dialect#getSequences()
	 */
	@Override
	public Map<String, Long> getSequences(Connection connection) throws SQLException
	{
		Map<String, Long> sequenceMap = new HashMap<String, Long>();
		
		String catalog = connection.getCatalog();
		
		ResultSet resultSet = connection.getMetaData().getTables((catalog != null) ? catalog : "", null, "%", new String[] { "SEQUENCE" });
		
		while (resultSet.next())
		{
			StringBuilder builder = new StringBuilder();
			
			String schema = resultSet.getString("TABLE_SCHEM");
			
			if (schema != null)
			{
				builder.append(schema).append(".");
			}
			
			sequenceMap.put(builder.append(resultSet.getString("TABLE_NAME")).toString(), null);
		}
		
		resultSet.close();
		
		resultSet = connection.createStatement().executeQuery("SELECT CURRVAL('" + Strings.join(sequenceMap.keySet(), "'), CURRVAL('") + "')");
		
		resultSet.next();
		
		int index = 0;
		
		for (String sequence: sequenceMap.keySet())
		{
			sequenceMap.put(sequence, resultSet.getLong(++index));
		}

		resultSet.getStatement().close();
		
		return sequenceMap;
	}
	
	/**
	 * @see net.sf.hajdbc.dialect.DefaultDialect#truncateTableFormat()
	 */
	@Override
	protected String truncateTableFormat()
	{
		return "TRUNCATE TABLE {0}";
	}

	/**
	 * @see net.sf.hajdbc.dialect.DefaultDialect#sequencePattern()
	 */
	@Override
	protected String sequencePattern()
	{
		return "(?:(?:CURR)|(?:NEXT))VAL\\s*\\(\\s*'(\\S+\\)'\\s*)";
	}
}
