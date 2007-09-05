/*
 * Copyright (c) 2004-2007, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc.dialect;

import java.sql.SQLException;

import net.sf.hajdbc.Dialect;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 */
@SuppressWarnings("nls")
public class TestMckoiDialect extends TestStandardDialect
{
	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#createDialect()
	 */
	@Override
	protected Dialect createDialect()
	{
		return new MckoiDialect();
	}

	/**
	 * @see net.sf.hajdbc.Dialect#parseInsertTable(java.lang.String)
	 */
	@Override
	@Test(dataProvider = "insert-table-sql")
	public String parseInsertTable(String sql) throws SQLException
	{
		this.replay();
		
		String table = this.dialect.parseInsertTable(sql);
		
		this.verify();

		assert table == null : table;

		return table;
	}

	@Override
	@DataProvider(name = "sequence-sql")
	Object[][] sequenceSQLProvider()
	{
		return new Object[][] {
			new Object[] { "SELECT NEXTVAL('success')" },
			new Object[] { "SELECT NEXTVAL ( 'success' )" },
			new Object[] { "SELECT CURRVAL('success')" },
			new Object[] { "SELECT CURRVAL ( 'success' )" },
			new Object[] { "SELECT NEXTVAL('success'), * FROM table" },
			new Object[] { "SELECT NEXTVAL ( 'success' ) , * FROM table" },
			new Object[] { "SELECT CURRVAL('success'), * FROM table" },
			new Object[] { "SELECT CURRVAL ( 'success' ) , * FROM table" },
			new Object[] { "INSERT INTO table VALUES (NEXTVAL('success'), 0)" },
			new Object[] { "INSERT INTO table VALUES (NEXTVAL ( 'success' ) , 0)" },
			new Object[] { "INSERT INTO table VALUES (CURRVAL('success'), 0)" },
			new Object[] { "INSERT INTO table VALUES (CURRVAL ( 'success' ) , 0)" },
			new Object[] { "UPDATE table SET id = NEXTVAL('success')" },
			new Object[] { "UPDATE table SET id = NEXTVAL ( 'success' )" },
			new Object[] { "UPDATE table SET id = CURRVAL('success')" },
			new Object[] { "UPDATE table SET id = CURRVAL ( 'success' )" },
			new Object[] { "SELECT * FROM table" },
		};
	}
}
