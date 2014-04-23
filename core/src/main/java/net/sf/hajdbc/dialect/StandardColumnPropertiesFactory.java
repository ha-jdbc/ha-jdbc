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

import net.sf.hajdbc.AbstractNamed;
import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.ColumnPropertiesFactory;
import net.sf.hajdbc.IdentifierNormalizer;

public class StandardColumnPropertiesFactory implements ColumnPropertiesFactory
{
	private final IdentifierNormalizer normalizer;
	
	public StandardColumnPropertiesFactory(IdentifierNormalizer normalizer)
	{
		this.normalizer = normalizer;
	}
	
	@Override
	public ColumnProperties createColumnProperties(String name, int type, String nativeType, String defaultValue, final String remarks, final Boolean autoIncrement)
	{
		return new StandardColumnProperties(this.normalizer.normalize(name), type, nativeType, autoIncrement);
	}

	private static class StandardColumnProperties extends AbstractNamed<String, ColumnProperties> implements ColumnProperties
	{
		private final int type;
		private final String nativeType;
		private final boolean autoIncrement;
		
		StandardColumnProperties(String name, int type, String nativeType, boolean autoIncrement)
		{
			super(name);
			this.type = type;
			this.nativeType = nativeType;
			this.autoIncrement = autoIncrement;
		}

		@Override
		public int getType()
		{
			return this.type;
		}

		@Override
		public String getNativeType()
		{
			return this.nativeType;
		}

		@Override
		public boolean isAutoIncrement()
		{
			return this.autoIncrement;
		}
	}
}
