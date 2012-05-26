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
import java.util.Map.Entry;

import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.QualifiedName;
import net.sf.hajdbc.SequenceProperties;
import net.sf.hajdbc.TableProperties;
import net.sf.hajdbc.cache.AbstractDatabaseProperties;
import net.sf.hajdbc.cache.DatabaseMetaDataSupport;

/**
 * @author Paul Ferraro
 *
 */
public class EagerDatabaseProperties extends AbstractDatabaseProperties
{
	private final Map<QualifiedName, TableProperties> tables = new HashMap<QualifiedName, TableProperties>();
	private final Map<QualifiedName, SequenceProperties> sequences = new HashMap<QualifiedName, SequenceProperties>();
	private final List<String> defaultSchemas;
	private final Map<Integer, Map.Entry<String, Integer>> types;
	
	public EagerDatabaseProperties(DatabaseMetaData metaData, DatabaseMetaDataSupport support, Dialect dialect) throws SQLException
	{
		super(metaData, support);
		
		Collection<QualifiedName> tables = support.getTables(metaData);
		
		for (QualifiedName table: tables)
		{
			TableProperties properties = new EagerTableProperties(metaData, support, table);
			
			this.tables.put(properties.getName(), properties);
		}
		
		List<String> defaultSchemaList = dialect.getDefaultSchemas(metaData);
		
		this.defaultSchemas = new ArrayList<String>(defaultSchemaList);
		
		for (SequenceProperties sequence: support.getSequences(metaData))
		{
			this.sequences.put(sequence.getName(), sequence);
		}
		
		this.types = support.getTypes(metaData);
	}
	
	@Override
	protected List<String> defaultSchemas()
	{
		return this.defaultSchemas;
	}

	@Override
	protected Map<QualifiedName, SequenceProperties> sequences()
	{
		return this.sequences;
	}

	@Override
	protected Map<QualifiedName, TableProperties> tables()
	{
		return this.tables;
	}

	@Override
	protected Map<Integer, Entry<String, Integer>> types()
	{
		return this.types;
	}
}
