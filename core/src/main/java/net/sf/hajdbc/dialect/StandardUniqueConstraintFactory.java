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

import net.sf.hajdbc.IdentifierNormalizer;
import net.sf.hajdbc.QualifiedName;
import net.sf.hajdbc.UniqueConstraint;
import net.sf.hajdbc.UniqueConstraintFactory;

public class StandardUniqueConstraintFactory implements UniqueConstraintFactory
{
	private final IdentifierNormalizer normalizer;
	
	public StandardUniqueConstraintFactory(IdentifierNormalizer normalizer)
	{
		this.normalizer = normalizer;
	}
	
	@Override
	public UniqueConstraint createUniqueConstraint(String name, final QualifiedName table)
	{
		return new StandardUniqueConstraint(this.normalizer.normalize(name), table, new IdentifierList(this.normalizer));
	}
	
	private static class StandardUniqueConstraint extends AbstractConstraint<UniqueConstraint> implements UniqueConstraint
	{
		StandardUniqueConstraint(String name, QualifiedName table, List<String> columns)
		{
			super(name, table, columns);
		}
	}
}
