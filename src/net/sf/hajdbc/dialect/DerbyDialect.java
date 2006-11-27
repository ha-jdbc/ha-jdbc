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

import java.text.MessageFormat;

import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.TableProperties;

/**
 * Dialect for <a href="http://db.apache.org/derby">Apache Derby</a>.
 * 
 * @author  Paul Ferraro
 * @since   1.1
 */
public class DerbyDialect extends StandardDialect
{
	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#executeFunctionFormat()
	 */
	@Override
	protected String executeFunctionFormat()
	{
		return "VALUES {0}";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#getLockTableSQL(net.sf.hajdbc.TableProperties)
	 */
	@Override
	public String getLockTableSQL(TableProperties properties)
	{
		return MessageFormat.format("LOCK TABLE {0} IN SHARE MODE", properties.getName());
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#isIdentity(net.sf.hajdbc.ColumnProperties)
	 */
	@Override
	public boolean isIdentity(ColumnProperties properties)
	{
		String remarks = properties.getRemarks();
		
		return (remarks != null) && remarks.contains("GENERATED ALWAYS AS IDENTITY");
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#supportsSequences()
	 */
	@Override
	public boolean supportsSequences()
	{
		return false;
	}

	/**
	 * Deferrability clause is not supported.
	 * @see net.sf.hajdbc.dialect.StandardDialect#createForeignKeyConstraintFormat()
	 */
	@Override
	protected String createForeignKeyConstraintFormat()
	{
		return "ALTER TABLE {1} ADD CONSTRAINT {0} FOREIGN KEY ({2}) REFERENCES {3} ({4}) ON DELETE {5,choice,0#CASCADE|1#RESTRICT|2#SET NULL|3#NO ACTION|4#SET DEFAULT} ON UPDATE {6,choice,0#CASCADE|1#RESTRICT|2#SET NULL|3#NO ACTION|4#SET DEFAULT}";
	}
}
