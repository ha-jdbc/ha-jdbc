/*
 * Copyright (c) 2004-2006, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc.dialect;

import net.sf.hajdbc.Dialect;

import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
@Test
public class TestDB2Dialect extends TestDefaultDialect
{
	@Override
	protected Dialect createDialect()
	{
		return new DB2Dialect();
	}

	@Override
	public void testGetSimpleSQL()
	{
		String sql = this.dialect.getSimpleSQL();
		
		assert sql.equals("SELECT 1 FROM SYSIBM.SYSDUMMY") : sql;
	}
}
