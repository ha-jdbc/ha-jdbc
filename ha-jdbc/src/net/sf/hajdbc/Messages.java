/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2004 Paul Ferraro
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
public class Messages
{
	public static final String DATABASE_INACTIVE = "database-inactive";
	public static final String NO_ACTIVE_DATABASES = "no-active-databases";
	public static final String INVALID_DATABASE = "invalid-database";

	public static final String DATABASE_DEACTIVATED = "database-deactivated";
	public static final String DATABASE_ACTIVATED = "database-activated";

	public static final String DATABASE_VALIDATE_FAILED = "database-validate-failed";
	public static final String DATABASE_DEACTIVATE_FAILED = "database-deactivate-failed";
	public static final String INVALID_SYNC_STRATEGY = "invalid-sync-strategy";
	public static final String INVALID_PROPERTY = "invalid-property";
	public static final String INVALID_PROPERTY_VALUE = "invalid-property-value";
	public static final String DATABASE_ACTIVATE_FAILED = "database-activate-failed";
	public static final String DATABASE_SYNC_START = "database-sync-start";
	public static final String DATABASE_SYNC_END = "database-sync-end";
	public static final String CONNECTION_CLOSE_FAILED = "connection-close-failed";
	
	public static final String CONFIG_NOT_FOUND = "config-not-found";
	public static final String CONFIG_FAILED = "config-failed";
	public static final String STREAM_CLOSE_FAILED = "stream-close-failed";
	
	public static final String DRIVER_REGISTER_FAILED = "driver-register-failed";
	public static final String DRIVER_NOT_FOUND = "driver-not-found";
	public static final String JDBC_URL_REJECTED = "jdbc-url-rejected";
	
	public static final String JNDI_LOOKUP_FAILED = "jndi-lookup-failed";
	public static final String NOT_INSTANCE_OF = "not-instance-of";
	
	public static final String SQL_OBJECT_INIT_FAILED = "sql-object-init-failed";
	public static final String DATABASE_NOT_ACTIVE = "database-not-active";

	public static final String PRIMARY_KEY_REQUIRED = "primary-key-required";
	public static final String INSERT_COUNT = "insert-count";
	public static final String UPDATE_COUNT = "update-count";
	public static final String DELETE_COUNT = "delete-count";
	public static final String STATEMENT_FAILED = "statement-failed";
	
	public static final String DATABASE_COMMAND_RECEIVED = "database-command-received";
	public static final String DATABASE_COMMAND_FAILED = "database-command-failed";
	public static final String GROUP_MEMBER_JOINED = "group-member-joined";
	public static final String GROUP_MEMBER_LEFT = "group-member-left";
	
	private static ResourceBundle resource = ResourceBundle.getBundle(Messages.class.getName());
	
	/**
	 * Returns the localized message using the specified resource key.
	 * @param key a resource key
	 * @return a localized message
	 */
	public static String getMessage(String key)
	{
		return resource.getString(key);
	}
	
	/**
	 * Convenience method for single argument messages.
	 * @param key a resource key
	 * @param arg a message argument
	 * @return a localized message
	 */
	public static String getMessage(String key, Object arg)
	{
		return getMessage(key, new Object[] { arg });
	}
	
	/**
	 * Returns the localized message using the specified resource key and arguments.
	 * @param key a resource key
	 * @param args an array of arguments
	 * @return a formatted localized message
	 */
	public static String getMessage(String key, Object[] args)
	{
		return MessageFormat.format(getMessage(key), args);
	}
}
