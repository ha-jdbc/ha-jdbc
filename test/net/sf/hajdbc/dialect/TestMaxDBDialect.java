/*
 * Copyright (c) 2004-2006, Identity Theft 911, LLC.  All rights reserved.
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
public class TestMaxDBDialect extends TestDefaultDialect
{
	@Override
	protected Dialect createDialect()
	{
		return new MaxDBDialect();
	}

	@Override
	public void testGetSimpleSQL()
	{
		String sql = this.dialect.getSimpleSQL();
		
		assert sql.equals("SELECT 1 FROM DUAL") : sql;
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
