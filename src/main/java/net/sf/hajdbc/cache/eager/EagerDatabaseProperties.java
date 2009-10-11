/*
 * HA-JDBC: High-Availability JDBC
 * Copyright 2004-2009 Paul Ferraro
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
package net.sf.hajdbc.cache.eager;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.cache.AbstractDatabaseProperties;
import net.sf.hajdbc.cache.DatabaseMetaDataSupportFactory;
import net.sf.hajdbc.cache.QualifiedName;
import net.sf.hajdbc.cache.SequenceProperties;
import net.sf.hajdbc.cache.TableProperties;

/**
 * @author Paul Ferraro
 *
 */
public class EagerDatabaseProperties extends AbstractDatabaseProperties
{
	private Map<String, TableProperties> tableMap = new HashMap<String, TableProperties>();
	private Map<String, SequenceProperties> sequenceMap = new HashMap<String, SequenceProperties>();
	private List<String> defaultSchemaList;

	public EagerDatabaseProperties(DatabaseMetaData metaData, DatabaseMetaDataSupportFactory factory, Dialect dialect) throws SQLException
	{
		super(metaData, factory, dialect);
		
		Collection<QualifiedName> tables = this.support.getTables(metaData);
		
		for (QualifiedName table: tables)
		{
			TableProperties properties = new EagerTableProperties(metaData, this.support, table);
			
			this.tableMap.put(properties.getName(), properties);
		}
		
		List<String> defaultSchemaList = this.dialect.getDefaultSchemas(metaData);
		
		this.defaultSchemaList = new ArrayList<String>(defaultSchemaList);
		
		for (SequenceProperties sequence: this.support.getSequences(metaData))
		{
			this.sequenceMap.put(sequence.getName(), sequence);
		}
	}
	
	@Override
	protected List<String> getDefaultSchemaList() throws SQLException
	{
		return this.defaultSchemaList;
	}

	@Override
	protected Map<String, SequenceProperties> getSequenceMap() throws SQLException
	{
		return this.sequenceMap;
	}

	@Override
	protected Map<String, TableProperties> getTableMap() throws SQLException
	{
		return this.tableMap;
	}
}
