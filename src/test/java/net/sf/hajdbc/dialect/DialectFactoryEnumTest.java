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

import net.sf.hajdbc.Dialect;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Paul Ferraro
 *
 */
@SuppressWarnings("nls")
public class DialectFactoryEnumTest
{
	@Test
	public void validate()
	{
		this.validate(DialectFactoryEnum.DB2, "db2", DB2Dialect.class);
		this.validate(DialectFactoryEnum.DERBY, "derby", DerbyDialect.class);
		this.validate(DialectFactoryEnum.FIREBIRD, "firebird", FirebirdDialect.class);
		this.validate(DialectFactoryEnum.H2, "h2", H2Dialect.class);
		this.validate(DialectFactoryEnum.HSQLDB, "hsqldb", HSQLDBDialect.class);
		this.validate(DialectFactoryEnum.INGRES, "ingres", IngresDialect.class);
		this.validate(DialectFactoryEnum.MAXDB, "maxdb", MaxDBDialect.class);
		this.validate(DialectFactoryEnum.MCKOI, "mckoi", MckoiDialect.class);
		this.validate(DialectFactoryEnum.MYSQL, "mysql", MySQLDialect.class);
		this.validate(DialectFactoryEnum.ORACLE, "oracle", OracleDialect.class);
		this.validate(DialectFactoryEnum.POSTGRESQL, "postgresql", PostgreSQLDialect.class);
		this.validate(DialectFactoryEnum.STANDARD, "standard", StandardDialect.class);
		this.validate(DialectFactoryEnum.SYBASE, "sybase", SybaseDialect.class);
	}
	
	private void validate(DialectFactory factory, String string, Class<? extends Dialect> dialectClass)
	{
		Assert.assertEquals(string, factory.toString());
		Assert.assertSame(dialectClass, factory.createDialect().getClass());
	}
}
