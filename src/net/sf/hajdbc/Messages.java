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
package net.sf.hajdbc;

import java.text.MessageFormat;
import java.util.ResourceBundle;

/**
 * Provides localized access to log/error messages.
 * 
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public final class Messages
{
	public static final String CLUSTER_STATE_LOAD_FAILED = "cluster-state-load-failed";
	public static final String CLUSTER_STATE_STORE_FAILED = "cluster-state-store-failed";
	public static final String COMMAND_RECEIVED = "command-received";
	public static final String CONFIG_LOAD_FAILED = "config-load-failed";
	public static final String CONFIG_STORE_FAILED = "config-store-failed";
	public static final String CONFIG_NOT_FOUND = "config-not-found";
	public static final String DATABASE_ACTIVATE_FAILED = "database-activate-failed";
	public static final String DATABASE_ACTIVATED = "database-activated";
	public static final String DATABASE_ALREADY_EXISTS = "database-already-exists";
	public static final String DATABASE_DEACTIVATED = "database-deactivated";
	public static final String DATABASE_NOT_ACTIVE = "database-not-active";
	public static final String DATABASE_NOT_ALIVE = "database-not-alive";
	public static final String DATABASE_STILL_ACTIVE = "database-still-active";
	public static final String DATABASE_SYNC_END = "database-sync-end";
	public static final String DATABASE_SYNC_START = "database-sync-start";
	public static final String DELETE_COUNT = "delete-count";
	public static final String DRIVER_NOT_FOUND = "driver-not-found";
	public static final String DRIVER_REGISTER_FAILED = "driver-register-failed";
	public static final String GROUP_MEMBER_JOINED = "group-member-joined";
	public static final String GROUP_MEMBER_LEFT = "group-member-left";
	public static final String HA_JDBC_INIT = "ha-jdbc-init";
	public static final String INSERT_COUNT = "insert-count";
	public static final String INVALID_BALANCER = "invalid-balancer";
	public static final String INVALID_DATABASE = "invalid-database";
	public static final String INVALID_DATABASE_CLUSTER = "invalid-database-cluster";
	public static final String INVALID_META_DATA_CACHE = "invalid-meta-data-cache";
	public static final String INVALID_PROPERTY = "invalid-property";
	public static final String INVALID_PROPERTY_VALUE = "invalid-property-value";
	public static final String INVALID_SYNC_STRATEGY = "invalid-sync-strategy";
	public static final String JDBC_URL_REJECTED = "jdbc-url-rejected";
	public static final String JNDI_LOOKUP_FAILED = "jndi-lookup-failed";
	public static final String MBEAN_SERVER_NOT_FOUND = "mbean-server-not-found";
	public static final String NO_ACTIVE_DATABASES = "no-active-databases";
	public static final String PRIMARY_KEY_REQUIRED = "primary-key-required";
	public static final String SEQUENCE_OUT_OF_SYNC = "sequence-out-of-sync";
	public static final String SHUT_DOWN = "shut-down";
	public static final String SQL_OBJECT_INIT_FAILED = "sql-object-init-failed";
	public static final String STATEMENT_FAILED = "statement-failed";
	public static final String TABLE_LOOKUP_FAILED = "table-lookup-failed";
	public static final String TABLE_LOCK_ACQUIRE = "table-lock-acquire";
	public static final String TABLE_LOCK_RELEASE = "table-lock-release";
	public static final String UPDATE_COUNT = "update-count";
	public static final String WRITE_LOCK_FAILED = "write-lock-failed";
	
	private static ResourceBundle resource = ResourceBundle.getBundle(Messages.class.getName());
	
	/**
	 * Returns the localized message using the specified resource key and potential arguments.
	 * @param key a resource key
	 * @param args a variable number of arguments
	 * @return a localized message
	 */
	public static String getMessage(String key, Object... args)
	{
		String message = resource.getString(key);
		
		return (args.length == 0) ? message : MessageFormat.format(message, args);
	}
	
	private Messages()
	{
		// Hide constructor
	}
}
