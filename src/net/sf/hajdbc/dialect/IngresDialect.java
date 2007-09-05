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
import java.util.Collection;
import java.util.regex.Pattern;

/**
 * Dialect for <a href="http://opensource.ingres.com/projects/ingres/">Ingres</a>.
 * 
 * @author Paul Ferraro
 */
@SuppressWarnings("nls")
public class IngresDialect extends StandardDialect
{
	private Pattern legacySequencePattern = Pattern.compile("'?(\\w+)'?\\.(?:(?:CURR)|(?:NEXT))VAL", Pattern.CASE_INSENSITIVE);
	
	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#parseInsertTable(java.lang.String)
	 */
	@Override
	public String parseInsertTable(String sql)
	{
		return null;
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#getSequences(java.sql.Connection)
	 */
	@Override
	public Collection<String> getSequences(Connection connection) throws SQLException
	{
		return this.executeQuery(connection, "SELECT seq_name FROM iisequence");
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#parseSequence(java.lang.String)
	 */
	@Override
	public String parseSequence(String sql)
	{
		String sequence = super.parseSequence(sql);
		
		return (sequence != null) ? sequence : this.parse(this.legacySequencePattern, sql);
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#sequencePattern()
	 */
	@Override
	protected String sequencePattern()
	{
		return "(?:NEXT|CURRENT)\\s+VALUE\\s+FOR\\s+'?([^',\\s\\(\\)]+)";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#currentDatePattern()
	 */
	@Override
	protected String currentDatePattern()
	{
		return "CURRENT_DATE|DATE\\s*\\(\\s*'TODAY'\\s*\\)";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#currentTimePattern()
	 */
	@Override
	protected String currentTimePattern()
	{
		return "CURRENT_TIME|LOCAL_TIME";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#currentTimestampPattern()
	 */
	@Override
	protected String currentTimestampPattern()
	{
		return "CURRENT_TIMESTAMP|LOCAL_TIMESTAMP|DATE\\s*\\(\\s*'NOW'\\s*\\)";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#randomPattern()
	 */
	@Override
	protected String randomPattern()
	{
		return "RANDOMF\\s*\\(\\s*\\)";
	}
}
