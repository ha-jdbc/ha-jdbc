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

import java.sql.SQLException;

import net.sf.hajdbc.Dialect;

import org.easymock.EasyMock;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
@Test
public class TestPostgreSQLDialect extends TestDefaultDialect
{
	@Override
	protected Dialect createDialect()
	{
		return new PostgreSQLDialect();
	}

	@Override
	public void testGetLockTableSQL()
	{
		String schema = "schema";
		String table = "table";
		String quote = "'";
		
		try
		{
			EasyMock.expect(this.metaData.supportsSchemasInDataManipulation()).andReturn(true);
			EasyMock.expect(this.metaData.getIdentifierQuoteString()).andReturn(quote).times(2);
			
			this.control.replay();
			
			String sql = this.dialect.getLockTableSQL(this.metaData, schema, table);
			
			this.control.verify();
			
			assert sql.equals("LOCK TABLE 'schema'.'table' IN EXCLUSIVE MODE") : sql;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	@Override
	public void testGetTruncateTableSQL()
	{
		String schema = "schema";
		String table = "table";
		String quote = "'";
		
		try
		{
			EasyMock.expect(this.metaData.supportsSchemasInDataManipulation()).andReturn(true);
			EasyMock.expect(this.metaData.getIdentifierQuoteString()).andReturn(quote).times(2);
			
			this.control.replay();
			
			String sql = this.dialect.getTruncateTableSQL(this.metaData, schema, table);
			
			this.control.verify();
			
			assert sql.equals("TRUNCATE TABLE 'schema'.'table'") : sql;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}
}
