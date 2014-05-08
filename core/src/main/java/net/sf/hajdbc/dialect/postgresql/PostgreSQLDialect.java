/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.sf.hajdbc.dialect.postgresql;

import java.io.File;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.DumpRestoreSupport;
import net.sf.hajdbc.IdentityColumnSupport;
import net.sf.hajdbc.SequenceSupport;
import net.sf.hajdbc.TriggerSupport;
import net.sf.hajdbc.codec.Decoder;
import net.sf.hajdbc.dialect.ConnectionProperties;
import net.sf.hajdbc.dialect.StandardDialect;
import net.sf.hajdbc.util.Processes;
import net.sf.hajdbc.util.Strings;

/**
 * Dialect for <a href="http://postgresql.org">PostgreSQL</a>.
 * @author  Paul Ferraro
 * @since   1.1
 */
@SuppressWarnings("nls")
public class PostgreSQLDialect extends StandardDialect implements DumpRestoreSupport
{
	// Taken from: http://www.postgresql.org/docs/9.2/static/sql-keywords-appendix.html
	public static final String[] RESERVED_KEY_WORDS = new String[] {
		"ALL", "ANALYSE", "ANALYZE", "AND", "ANY", "ARRAY", "AS", "ASC", "ASYMMETRIC", "AUTHORIZATION",
		"BINARY", "BOTH",
		"CASE", "CAST", "CHECK", "COLLATE", "COLLATION", "COLUMN", "CONCURRENTLY", "CONSTRAINT", "CREATE", "CROSS", "CURRENT_CATALOG", "CURRENT_DATE", "CURRENT_ROLE", "CURRENT_SCHEMA", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER",
		"DEFAULT", "DEFERRABLE", "DESC", "DISTINCT", "DO",
		"ELSE", "END", "EXCEPT",
		"FALSE", "FETCH", "FOR", "FOREIGN", "FREEZE", "FROM", "FULL",
		"GRANT", "GROUP",
		"HAVING",
		"ILIKE", "IN", "INITIALLY", "INNER", "INTERSECT", "INTO", "IS", "ISNULL",
		"JOIN",
		"LEADING", "LEFT", "LIKE", "LIMIT", "LOCALTIME", "LOCALTIMESTAMP",
		"NATURAL", "NOT", "NOTNULL", "NULL",
		"OFFSET", "ON", "ONLY", "OR", "ORDER", "OUTER", "OVER", "OVERLAPS",
		"PLACING", "PRIMARY",
		"REFERENCES", "RETURNING", "RIGHT",
		"SELECT", "SESSION_USER", "SIMILAR", "SOME", "SYMMETRIC",
		"TABLE", "THEN", "TO", "TRAILING", "TRUE",
		"UNION", "UNIQUE", "USER", "USING",
		"VARIADIC", "VERBOSE",
		"WHEN", "WHERE", "WINDOW", "WITH",
	};

	private static final File PASSWORD_FILE = new File(String.format("%s%s.pgpass", Strings.USER_HOME, Strings.FILE_SEPARATOR));
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialect#vendorPattern()
	 */
	@Override
	protected String vendorPattern()
	{
		return "postgresql";
	}

	/**
	 * PostgreSQL uses a schema search path to locate unqualified table names.
	 * The default search path is [$user,public], where $user is the current user.
	 * @see net.sf.hajdbc.dialect.StandardDialect#getDefaultSchemas(java.sql.DatabaseMetaData)
	 */
	@Override
	public List<String> getDefaultSchemas(DatabaseMetaData metaData) throws SQLException
	{
		try (Statement statement = metaData.getConnection().createStatement())
		{
			try (ResultSet resultSet = statement.executeQuery("SHOW search_path"))
			{
				resultSet.next();
				
				String[] schemas = resultSet.getString(1).split(Strings.COMMA);
				
				List<String> schemaList = new ArrayList<>(schemas.length);
				
				for (String schema: schemas)
				{
					schemaList.add(schema.equals("$user") ? metaData.getUserName() : schema);
				}
				
				return schemaList;
			}
		}
	}

	/**
	 * PostgreSQL uses the native type OID to identify BLOBs.
	 * However the JDBC driver incomprehensibly maps OIDs to INTEGERs.
	 * The PostgreSQL JDBC folks claim this intentional.
	 * @see net.sf.hajdbc.dialect.StandardDialect#getColumnType(net.sf.hajdbc.ColumnProperties)
	 */
	@Override
	public int getColumnType(ColumnProperties properties)
	{
		return properties.getNativeType().equalsIgnoreCase("oid") ? Types.BLOB : properties.getType();
	}

	@Override
	protected Pattern identifierPattern(DatabaseMetaData metaData) throws SQLException
	{
		if ((metaData.getDriverMajorVersion() >= 8) && (metaData.getDriverMinorVersion() >= 1))
		{
			return Pattern.compile("[A-Za-z\\0200-\\0377_][A-Za-z\\0200-\\0377_0-9\\$]*");
		}
		return super.identifierPattern(metaData);
	}

	@Override
	protected Set<String> reservedIdentifiers(DatabaseMetaData metaData)
	{
		return new HashSet<>(Arrays.asList(RESERVED_KEY_WORDS));
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialect#getSequenceSupport()
	 */
	@Override
	public SequenceSupport getSequenceSupport()
	{
		return this;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialect#getIdentityColumnSupport()
	 */
	@Override
	public IdentityColumnSupport getIdentityColumnSupport()
	{
		return this;
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#truncateTableFormat()
	 */
	@Override
	protected String truncateTableFormat()
	{
		return "TRUNCATE TABLE {0}";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#sequencePattern()
	 */
	@Override
	protected String sequencePattern()
	{
		return "(?:CURR|NEXT)VAL\\s*\\(\\s*'([^']+)'\\s*\\)";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#nextSequenceValueFormat()
	 */
	@Override
	protected String nextSequenceValueFormat()
	{
		return "NEXTVAL(''{0}'')";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#alterIdentityColumnFormat()
	 */
	@Override
	protected String alterIdentityColumnFormat()
	{
		return "ALTER SEQUENCE {0}_{1}_seq RESTART WITH {2}";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#currentTimestampPattern()
	 */
	@Override
	protected String currentTimestampPattern()
	{
		return super.currentTimestampPattern() + "|(?<=\\W)NOW\\s*\\(\\s*\\)|(?<=\\W)TRANSACTION_TIMESTAMP\\s*\\(\\s*\\)|(?<=\\W)STATEMENT_TIMESTAMP\\s*\\(\\s*\\)|(?<=\\W)CLOCK_TIMESTAMP\\s*\\(\\s*\\)";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#randomPattern()
	 */
	@Override
	protected String randomPattern()
	{
		return "(?<=\\W)RANDOM\\s*\\(\\s*\\)";
	}

	/**
	 * Recognizes FOR SHARE and FOR UPDATE.
	 * @see net.sf.hajdbc.dialect.StandardDialect#selectForUpdatePattern()
	 */
	@Override
	protected String selectForUpdatePattern()
	{
		return "SELECT\\s+.+\\s+FOR\\s+(SHARE|UPDATE)";
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialect#getDumpRestoreSupport()
	 */
	@Override
	public DumpRestoreSupport getDumpRestoreSupport()
	{
		return this;
	}

	@Override
	public <Z, D extends Database<Z>> void dump(D database, Decoder decoder, File file, boolean dataOnly) throws Exception
	{
		ConnectionProperties properties = this.getConnectionProperties(database, decoder);
		ProcessBuilder builder = new ProcessBuilder("pg_dump");
		List<String> args = builder.command();
		args.add("--host=" + properties.getHost());
		args.add("--port=" + properties.getPort());
		args.add("--username=" + properties.getUser());
		args.add("--no-password");
		args.add("--file=" + file.getPath());
		args.add("--format=tar");
		args.add(properties.getDatabase());
		Processes.run(setPassword(builder, properties));
	}

	@Override
	public <Z, D extends Database<Z>> void restore(D database, Decoder decoder, File file, boolean dataOnly) throws Exception
	{
		ConnectionProperties properties = this.getConnectionProperties(database, decoder);
		ProcessBuilder builder = new ProcessBuilder("pg_restore");
		List<String> args = builder.command();
		args.add("--host=" + properties.getHost());
		args.add("--port=" + properties.getPort());
		args.add("--username=" + properties.getUser());
		args.add("--no-password");
		args.add("--dbname=" + properties.getDatabase());
		args.add("--clean");
		if (dataOnly)
		{
			args.add("--data-only");
			args.add("--disable-triggers"); // Used to prevent integrity constraints during restoration of data
			args.add("--superuser=" + properties.getUser()); // Required by --disable-triggers
		}
		args.add(file.getPath());
		Processes.run(setPassword(builder, properties));
	}
	
	private static ProcessBuilder setPassword(ProcessBuilder builder, ConnectionProperties properties)
	{
		String password = properties.getPassword();
		if ((password != null) && !PASSWORD_FILE.exists())
		{
			Processes.environment(builder).put("PGPASSWORD", properties.getPassword());
		}
		return builder;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialect#getTriggerSupport()
	 */
	@Override
	public TriggerSupport getTriggerSupport()
	{
		return this;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialect#createForeignKeyConstraintFormat()
	 */
	@Override
	protected String createTriggerFormat()
	{
		return "CREATE FUNCTION {0}_action() BEGIN {4} END; CREATE TRIGGER {0} {1} {2} ON {3} FOR EACH ROW EXECUTE PROCEDURE {0}_action()";
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialect#dropTriggerFormat()
	 */
	@Override
	protected String dropTriggerFormat()
	{
		return "DROP TRIGGER {0} ON {1}; DROP FUNCTION {0}_action()";
	}
}
