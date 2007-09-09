/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2007 Paul Ferraro
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
package net.sf.hajdbc.cache;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.hajdbc.DatabaseProperties;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.QualifiedName;
import net.sf.hajdbc.SequenceProperties;
import net.sf.hajdbc.TableProperties;

/**
 * @author Paul Ferraro
 *
 */
public class LazyDatabaseProperties implements DatabaseProperties
{
	private static ThreadLocal<Connection> threadLocal = new ThreadLocal<Connection>();
	
	private Map<String, TableProperties> tableMap;
	private Map<String, SequenceProperties> sequenceMap;
	private Boolean supportsSelectForUpdate;
	private DatabaseMetaDataSupport support;
	private List<String> defaultSchemaList;
	private Dialect dialect;

	public LazyDatabaseProperties(Dialect dialect) throws SQLException
	{
		this.support = new DatabaseMetaDataSupport(getDatabaseMetaData(), dialect);
		this.dialect = dialect;
	}

	public static void setConnection(Connection connection)
	{
		threadLocal.set(connection);
	}
	
	public static DatabaseMetaData getDatabaseMetaData() throws SQLException
	{
		return threadLocal.get().getMetaData();
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseProperties#getTables()
	 */
	@Override
	public synchronized Collection<TableProperties> getTables() throws SQLException
	{
		return this.getTableMap().values();
	}

	private synchronized Map<String, TableProperties> getTableMap() throws SQLException
	{
		if (this.tableMap == null)
		{
			this.tableMap = new HashMap<String, TableProperties>();
			
			for (QualifiedName table: this.support.getTables(getDatabaseMetaData()))
			{
				TableProperties properties = new LazyTableProperties(this.support, table);
				
				this.tableMap.put(properties.getName(), properties);
			}
		}
		
		return this.tableMap;
	}
	
	private synchronized Map<String, SequenceProperties> getSequenceMap() throws SQLException
	{
		if (this.sequenceMap == null)
		{
			this.sequenceMap = new HashMap<String, SequenceProperties>();
			
			for (SequenceProperties sequence: this.support.getSequences(getDatabaseMetaData()))
			{
				this.sequenceMap.put(sequence.getName(), sequence);
			}
		}
		
		return this.sequenceMap;
	}
	
	private synchronized List<String> getDefaultSchemaList() throws SQLException
	{
		if (this.defaultSchemaList == null)
		{
			this.defaultSchemaList = this.dialect.getDefaultSchemas(getDatabaseMetaData());
		}
		
		return this.defaultSchemaList;
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseProperties#findTable(java.lang.String)
	 */
	@Override
	public TableProperties findTable(String table) throws SQLException
	{
		return this.support.find(this.getTableMap(), table, this.getDefaultSchemaList());
	}

	/**
	 * @see net.sf.hajdbc.DatabaseProperties#supportsSelectForUpdate()
	 */
	@Override
	public synchronized boolean supportsSelectForUpdate() throws SQLException
	{
		if (this.supportsSelectForUpdate == null)
		{
			this.supportsSelectForUpdate = getDatabaseMetaData().supportsSelectForUpdate();
		}
		
		return this.supportsSelectForUpdate;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseProperties#findSequence(java.lang.String)
	 */
	@Override
	public SequenceProperties findSequence(String sequence) throws SQLException
	{
		return this.support.find(this.getSequenceMap(), sequence, this.getDefaultSchemaList());
	}

	/**
	 * @see net.sf.hajdbc.DatabaseProperties#getSequences()
	 */
	@Override
	public Collection<SequenceProperties> getSequences() throws SQLException
	{
		return this.getSequenceMap().values();
	}
}
