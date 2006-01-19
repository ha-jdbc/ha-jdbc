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
 * Dialect for Apache Derby (open-source).
 * <p><a href="http://db.apache.org/derby">http://db.apache.org/derby</a></p>
 * 
 * @author  Paul Ferraro
 * @since   1.1
 */
public class DerbyDialect extends DefaultDialect
{
	/**
	 * @see net.sf.hajdbc.dialect.DefaultDialect#getSimpleSQL()
	 */
	public String getSimpleSQL()
	{
		return "VALUES CURRENT_TIMESTAMP";
	}
}
