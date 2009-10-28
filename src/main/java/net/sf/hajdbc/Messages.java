/*
 * HA-JDBC: High-Availability JDBC
 * Copyright 2004-2009 Paul Ferraro
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
package net.sf.hajdbc;

import java.text.MessageFormat;
import java.util.ResourceBundle;

/**
 * Provides localized access to log/error messages.
 * 
 * @author  Paul Ferraro
 * @version $Revision: 1948 $
 * @since   1.0
 */
@SuppressWarnings("nls")
public enum Messages
{
	CLUSTER_NOT_ACTIVE("cluster-not-active"),
	CLUSTER_PANIC_DETECTED("cluster-panic-detected"),
	CLUSTER_START_FAILED("cluster-start-failed"),
	CLUSTER_STATE_LOAD_FAILED("cluster-state-load-failed"),
	CLUSTER_STATE_STORE_FAILED("cluster-state-store-failed"),
	COMMAND_RECEIVED("command-received"),
	CONFIG_LOAD_FAILED("config-load-failed"),
	CONFIG_STORE_FAILED("config-store-failed"),
	CONFIG_NOT_FOUND("config-not-found"),
	DATABASE_ACTIVATE_FAILED("database-activate-failed"),
	DATABASE_ACTIVATED("database-activated"),
	DATABASE_ALREADY_EXISTS("database-already-exists"),
	DATABASE_DEACTIVATED("database-deactivated"),
	DATABASE_INCONSISTENT("database-inconsistent"),
	DATABASE_NOT_ACTIVE("database-not-active"),
	DATABASE_NOT_ALIVE("database-not-alive"),
	DATABASE_STILL_ACTIVE("database-still-active"),
	DATABASE_SYNC_END("database-sync-end"),
	DATABASE_SYNC_START("database-sync-start"),
	DELETE_COUNT("delete-count"),
	DRIVER_NOT_FOUND("driver-not-found"),
	DRIVER_REGISTER_FAILED("driver-register-failed"),
	GROUP_MEMBER_JOINED("group-member-joined"),
	GROUP_MEMBER_LEFT("group-member-left"),
	HA_JDBC_INIT("ha-jdbc-init"),
	INITIAL_CLUSTER_STATE_LOCAL("initial-cluster-state-local"),
	INITIAL_CLUSTER_STATE_NONE("initial-cluster-state-none"),
	INITIAL_CLUSTER_STATE_REMOTE("initial-cluster-state-remote"),
	INSERT_COUNT("insert-count"),
	INVALID_DATABASE("invalid-database"),
	INVALID_DATABASE_CLUSTER("invalid-database-cluster"),
	INVALID_PROPERTY("invalid-property"),
	INVALID_PROPERTY_VALUE("invalid-property-value"),
	INVALID_SYNC_STRATEGY("invalid-sync-strategy"),
	JDBC_URL_REJECTED("jdbc-url-rejected"),
	JNDI_LOOKUP_FAILED("jndi-lookup-failed"),
	MBEAN_SERVER_NOT_FOUND("mbean-server-not-found"),
	NO_ACTIVE_DATABASES("no-active-databases"),
	PRIMARY_KEY_REQUIRED("primary-key-required"),
	SCHEMA_LOOKUP_FAILED("schema-lookup-failed"),
	SEQUENCE_OUT_OF_SYNC("sequence-out-of-sync"),
	SHUT_DOWN("shut-down"),
	SQL_OBJECT_INIT_FAILED("sql-object-init-failed"),
	STATEMENT_FAILED("statement-failed"),
	TABLE_LOCK_ACQUIRE("table-lock-acquire"),
	TABLE_LOCK_RELEASE("table-lock-release"),
	UPDATE_COUNT("update-count"),
	WRITE_LOCK_FAILED("write-lock-failed");
	
	private static ResourceBundle resource = ResourceBundle.getBundle(Messages.class.getName());
	
	private String key;
	
	private Messages(String key)
	{
		this.key = key;
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.lang.Enum#toString()
	 */
	@Override
	public String toString()
	{
		return this.key;
	}

	/**
	 * Returns the localized message using the supplied arguments.
	 * @param args a variable number of arguments
	 * @return a localized message
	 */
	public String getMessage(Object... args)
	{
		String pattern = resource.getString(this.key);
		
		return (args.length == 0) ? pattern : MessageFormat.format(pattern, args);
	}
}
