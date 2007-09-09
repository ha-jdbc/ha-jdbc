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

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
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
public class EagerDatabaseProperties implements DatabaseProperties
{
	private DatabaseMetaDataSupport support;
	private Map<String, TableProperties> tableMap = new HashMap<String, TableProperties>();
	private Map<String, SequenceProperties> sequenceMap = new HashMap<String, SequenceProperties>();
	private boolean supportsSelectForUpdate;
	private List<String> defaultSchemaList;
	
	public EagerDatabaseProperties(DatabaseMetaData metaData, Dialect dialect) throws SQLException
	{
		this.support = new DatabaseMetaDataSupport(metaData, dialect);
		
		this.supportsSelectForUpdate = metaData.supportsSelectForUpdate();
		
		Collection<QualifiedName> tables = this.support.getTables(metaData);
		
		for (QualifiedName table: tables)
		{
			TableProperties properties = new EagerTableProperties(metaData, this.support, table);
			
			this.tableMap.put(properties.getName(), properties);
		}
		
		List<String> defaultSchemaList = dialect.getDefaultSchemas(metaData);
		
		this.defaultSchemaList = new ArrayList<String>(defaultSchemaList);
		
		for (SequenceProperties sequence: this.support.getSequences(metaData))
		{
			this.sequenceMap.put(sequence.getName(), sequence);
		}
	}

	/**
	 * @see net.sf.hajdbc.DatabaseProperties#getTables()
	 */
	@Override
	public Collection<TableProperties> getTables()
	{
		return this.tableMap.values();
	}

	/**
	 * @see net.sf.hajdbc.DatabaseProperties#findTable(java.lang.String)
	 */
	@Override
	public TableProperties findTable(String table) throws SQLException
	{
		return this.support.find(this.tableMap, table, this.defaultSchemaList);
	}

	/**
	 * @see net.sf.hajdbc.DatabaseProperties#supportsSelectForUpdate()
	 */
	@Override
	public boolean supportsSelectForUpdate()
	{
		return this.supportsSelectForUpdate;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseProperties#findSequence(java.lang.String)
	 */
	@Override
	public SequenceProperties findSequence(String sequence) throws SQLException
	{
		return this.support.find(this.sequenceMap, sequence, this.defaultSchemaList);
	}

	/**
	 * @see net.sf.hajdbc.DatabaseProperties#getSequences()
	 */
	@Override
	public Collection<SequenceProperties> getSequences() throws SQLException
	{
		return this.sequenceMap.values();
	}
}
