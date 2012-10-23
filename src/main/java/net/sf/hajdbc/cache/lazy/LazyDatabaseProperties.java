/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
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
package net.sf.hajdbc.cache.lazy;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;

import net.sf.hajdbc.QualifiedName;
import net.sf.hajdbc.SequenceProperties;
import net.sf.hajdbc.SequencePropertiesFactory;
import net.sf.hajdbc.SequenceSupport;
import net.sf.hajdbc.TableProperties;
import net.sf.hajdbc.cache.AbstractDatabaseProperties;
import net.sf.hajdbc.cache.DatabaseMetaDataProvider;
import net.sf.hajdbc.dialect.Dialect;

/**
 * @author Paul Ferraro
 *
 */
public class LazyDatabaseProperties extends AbstractDatabaseProperties
{
	private final Dialect dialect;
	private final DatabaseMetaDataProvider provider;
	private final SequencePropertiesFactory sequenceFactory;
	
	private final AtomicReference<Map<QualifiedName, TableProperties>> tablesRef = new AtomicReference<Map<QualifiedName, TableProperties>>();
	private final AtomicReference<Map<QualifiedName, SequenceProperties>> sequencesRef = new AtomicReference<Map<QualifiedName, SequenceProperties>>();
	private final AtomicReference<List<String>> defaultSchemasRef = new AtomicReference<List<String>>();
	private final AtomicReference<Map<Integer, Map.Entry<String, Integer>>> typesRef = new AtomicReference<Map<Integer, Map.Entry<String, Integer>>>();
	
	public LazyDatabaseProperties(DatabaseMetaDataProvider provider, Dialect dialect) throws SQLException
	{
		super(provider.getDatabaseMetaData(), dialect);

		this.provider = provider;
		this.dialect = dialect;
		SequenceSupport support = dialect.getSequenceSupport();
		this.sequenceFactory = (support != null) ? support.createSequencePropertiesFactory(this.nameFactory) : null;
	}
	
	@Override
	protected Map<QualifiedName, TableProperties> tables() throws SQLException
	{
		Map<QualifiedName, TableProperties> tables = this.tablesRef.get();
		
		if (tables == null)
		{
			tables = new HashMap<QualifiedName, TableProperties>();
			
			for (QualifiedName table: this.dialect.getTables(this.provider.getDatabaseMetaData(), this.nameFactory))
			{
				TableProperties properties = new LazyTableProperties(table, this.provider, this.dialect, this.nameFactory);
				
				tables.put(properties.getName(), properties);
			}
			
			if (!this.tablesRef.compareAndSet(null, tables))
			{
				return this.tablesRef.get();
			}
		}
		
		return tables;
	}
	
	@Override
	protected Map<QualifiedName, SequenceProperties> sequences() throws SQLException
	{
		Map<QualifiedName, SequenceProperties> sequences = this.sequencesRef.get();

		if (sequences == null)
		{
			sequences = new HashMap<QualifiedName, SequenceProperties>();
			
			if (this.sequenceFactory != null)
			{
				for (SequenceProperties sequence: dialect.getSequenceSupport().getSequences(this.provider.getDatabaseMetaData(), this.sequenceFactory))
				{
					sequences.put(sequence.getName(), sequence);
				}
			}
			
			if (!this.sequencesRef.compareAndSet(null, sequences))
			{
				return this.sequencesRef.get();
			}
		}

		return sequences;
	}
	
	@Override
	protected List<String> defaultSchemas() throws SQLException
	{
		List<String> schemas = this.defaultSchemasRef.get();
		
		if (schemas == null)
		{
			schemas = this.dialect.getDefaultSchemas(this.provider.getDatabaseMetaData());
			
			if (!this.defaultSchemasRef.compareAndSet(null, schemas))
			{
				return this.defaultSchemasRef.get();
			}
		}
		
		return schemas;
	}

	@Override
	protected Map<Integer, Entry<String, Integer>> types() throws SQLException
	{
		Map<Integer, Map.Entry<String, Integer>> types = this.typesRef.get();
		
		if (types == null)
		{
			types = this.dialect.getTypes(this.provider.getDatabaseMetaData());
			
			if (!this.typesRef.compareAndSet(null, types))
			{
				return this.typesRef.get();
			}
		}
		
		return types;
	}
}
