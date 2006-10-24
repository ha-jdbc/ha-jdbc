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

/**
 * @author Paul Ferraro
 *
 */
public class MySQLDialect extends DefaultDialect
{
	/**
	 * Deferrability clause is not supported.
	 * @see net.sf.hajdbc.dialect.DefaultDialect#createForeignKeyPattern()
	 */
	@Override
	protected String createForeignKeyPattern()
	{
		return "ALTER TABLE {1} ADD CONSTRAINT {0} FOREIGN KEY ({2}) REFERENCES {3} ({4}) ON DELETE {5,choice,0#CASCADE|1#RESTRICT|2#SET NULL|3#NO ACTION|4#SET DEFAULT} ON UPDATE {6,choice,0#CASCADE|1#RESTRICT|2#SET NULL|3#NO ACTION|4#SET DEFAULT}";
	}
	
	/**
	 * @see net.sf.hajdbc.dialect.DefaultDialect#createUniqueKeyPattern()
	 */
	@Override
	protected String createUniqueKeyPattern()
	{
		return "ALTER TABLE {1} ADD UNIQUE {0} ({2})";
	}

	/**
	 * @see net.sf.hajdbc.dialect.DefaultDialect#dropForeignKeyPattern()
	 */
	@Override
	protected String dropForeignKeyPattern()
	{
		return "ALTER TABLE {1} DROP FOREIGN KEY {0}";
	}

	/**
	 * @see net.sf.hajdbc.dialect.DefaultDialect#dropUniqueKeyPattern()
	 */
	@Override
	protected String dropUniqueKeyPattern()
	{
		return "ALTER TABLE {1} DROP INDEX {0}";
	}
}
