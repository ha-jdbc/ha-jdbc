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
package net.sf.hajdbc.dialect;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import net.sf.hajdbc.AbstractNamed;
import net.sf.hajdbc.IdentifierNormalizer;
import net.sf.hajdbc.QualifiedName;
import net.sf.hajdbc.QualifiedNameFactory;
import net.sf.hajdbc.util.Strings;

public class StandardQualifiedNameFactory implements QualifiedNameFactory
{
	private final IdentifierNormalizer normalizer;
	private final boolean supportsSchemasInDDL;
	private final boolean supportsSchemasInDML;
	
	public StandardQualifiedNameFactory(DatabaseMetaData metaData, IdentifierNormalizer normalizer) throws SQLException
	{
		this.supportsSchemasInDML = metaData.supportsSchemasInDataManipulation();
		this.supportsSchemasInDDL = metaData.supportsSchemasInTableDefinitions();
		this.normalizer = normalizer;
	}

	@Override
	public QualifiedName createQualifiedName(String schema, String name)
	{
		return new StandardQualifiedName(this.normalizer.normalize(schema), this.normalizer.normalize(name), this.supportsSchemasInDDL, this.supportsSchemasInDML);
	}

	@Override
	public IdentifierNormalizer getIdentifierNormalizer()
	{
		return this.normalizer;
	}

	@Override
	public QualifiedName parse(String raw)
	{
		int index = raw.indexOf(Strings.DOT);
		String schema = (index >= 0) ? raw.substring(0, index) : null;
		String name = (index >= 0) ? raw.substring(index + 1) : raw;
		return this.createQualifiedName(schema, name);
	}

	private class StandardQualifiedName extends AbstractNamed<String, QualifiedName> implements QualifiedName
	{
		private final String schema;
		private final String ddlName;
		private final String dmlName;
		
		StandardQualifiedName(String schema, String name, boolean supportsSchemasInDDL, boolean supportsSchemasInDML)
		{
			super(name);
			this.schema = schema;
			this.ddlName = this.qualify(supportsSchemasInDDL);
			this.dmlName = (supportsSchemasInDDL == supportsSchemasInDML) ? this.ddlName : this.qualify(supportsSchemasInDML);
		}

		@Override
		public String getSchema()
		{
			return this.schema;
		}

		@Override
		public String getDDLName()
		{
			return this.ddlName;
		}

		@Override
		public String getDMLName()
		{
			return this.dmlName;
		}

		private String qualify(boolean supportsSchemas)
		{
			return (supportsSchemas && (this.schema != null)) ? new StringBuilder(this.schema).append(Strings.DOT).append(this.getName()).toString() : this.getName();
		}
	}
}
