/*
 * Copyright (c) 2004-2006, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc.dialect;

import org.testng.annotations.Test;

import net.sf.hajdbc.Dialect;

/**
 * @author Paul Ferraro
 *
 */
@Test
public class TestDerbyDialect extends TestDefaultDialect
{
	@Override
	protected Dialect createDialect()
	{
		return new DerbyDialect();
	}

	@Override
	public void testGetSimpleSQL()
	{
		String sql = this.dialect.getSimpleSQL();
		
		assert sql.equals("VALUES 1") : sql;
	}
}
