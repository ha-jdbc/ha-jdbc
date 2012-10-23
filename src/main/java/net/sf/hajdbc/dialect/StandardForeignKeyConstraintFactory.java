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

import java.util.List;

import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.ForeignKeyConstraintFactory;
import net.sf.hajdbc.IdentifierNormalizer;
import net.sf.hajdbc.QualifiedName;
import net.sf.hajdbc.QualifiedNameFactory;

public class StandardForeignKeyConstraintFactory implements ForeignKeyConstraintFactory
{
	final QualifiedNameFactory factory;
	
	public StandardForeignKeyConstraintFactory(QualifiedNameFactory factory)
	{
		this.factory = factory;
	}
	
	@Override
	public QualifiedNameFactory getQualifiedNameFactory()
	{
		return this.factory;
	}

	@Override
	public ForeignKeyConstraint createForeignKeyConstraint(String name, final QualifiedName table, final QualifiedName foreignTable, final int deleteRule, final int updateRule, final int deferrability)
	{
		IdentifierNormalizer normalizer = this.factory.getIdentifierNormalizer();
		return new StandardForeignKeyConstraint(normalizer.normalize(name), table, new IdentifierList(normalizer), foreignTable, new IdentifierList(normalizer), deleteRule, updateRule, deferrability);
	}

	private static class StandardForeignKeyConstraint extends AbstractConstraint<ForeignKeyConstraint> implements ForeignKeyConstraint
	{
		private final QualifiedName foreignTable;
		private final List<String> foreignColumns;
		private final int deleteRule;
		private final int updateRule;
		private final int deferrability;
		
		StandardForeignKeyConstraint(String name, QualifiedName table, List<String> columns, QualifiedName foreignTable, List<String> foreignColumns, int deleteRule, int updateRule, int deferrability)
		{
			super(name, table, columns);
			this.foreignTable = foreignTable;
			this.foreignColumns = foreignColumns;
			this.deleteRule = deleteRule;
			this.updateRule = updateRule;
			this.deferrability = deferrability;
		}
		
		@Override
		public QualifiedName getForeignTable()
		{
			return this.foreignTable;
		}

		@Override
		public List<String> getForeignColumnList()
		{
			return this.foreignColumns;
		}

		@Override
		public int getDeleteRule()
		{
			return this.deleteRule;
		}

		@Override
		public int getUpdateRule()
		{
			return this.updateRule;
		}

		@Override
		public int getDeferrability()
		{
			return this.deferrability;
		}
	}
}
