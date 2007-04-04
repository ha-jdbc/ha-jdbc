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
package net.sf.hajdbc.dialect;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import net.sf.hajdbc.ColumnProperties;

/**
 * Dialect for <a href="http://www.mysql.com/products/database/mysql/">MySQL</a>
 * @author Paul Ferraro
 */
public class MySQLDialect extends StandardDialect
{
	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#getDefaultSchemas(java.sql.Connection)
	 */
	@Override
	public List<String> getDefaultSchemas(Connection connection) throws SQLException
	{
		return Collections.singletonList(this.executeFunction(connection, "DATABASE()"));
	}
	
	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#isIdentity(net.sf.hajdbc.ColumnProperties)
	 */
	@Override
	public boolean isIdentity(ColumnProperties properties)
	{
		String remarks = properties.getRemarks();
		
		return (remarks != null) && remarks.contains("AUTO_INCREMENT");
	}

	/**
	 * Support for identity columns is disabled.
	 * Although MySQL supports this feature, its in-memory implementation provides no mechanism to reset the counter during synchronization.
	 * @see net.sf.hajdbc.dialect.StandardDialect#supportsIdentityColumns()
	 */
	@Override
	public boolean supportsIdentityColumns()
	{
		return false;
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

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#createUniqueConstraintFormat()
	 */
	@Override
	protected String createUniqueConstraintFormat()
	{
		return "ALTER TABLE {1} ADD UNIQUE {0} ({2})";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#dropForeignKeyConstraintFormat()
	 */
	@Override
	protected String dropForeignKeyConstraintFormat()
	{
		return "ALTER TABLE {1} DROP FOREIGN KEY {0}";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#dropUniqueConstraintFormat()
	 */
	@Override
	protected String dropUniqueConstraintFormat()
	{
		return "ALTER TABLE {1} DROP INDEX {0}";
	}
}
