/*
 * HA-JDBC: High-Availability JDBC
 * Copyright  = C) 2014  Paul Ferraro
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *  = at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.sf.hajdbc.xml;


/**
 * XML constants
 * @author Paul Ferraro
 */
public interface Constants
{
	final String ROOT = "ha-jdbc";

	final String ALLOW_EMPTY_CLUSTER = "allow-empty-cluster";
	final String AUTO_ACTIVATE_SCHEDULE = "auto-activate-schedule";
	final String BALANCER = "balancer";
	final String CLUSTER = "cluster";
	final String DATABASE = "database";
	final String DEFAULT_SYNC = "default-sync";
	final String DETECT_IDENTITY_COLUMNS = "detect-identity-columns";
	final String DETECT_SEQUENCES = "detect-sequences";
	final String DIALECT = "dialect";
	final String DISTRIBUTABLE = "distributable";
	final String DURABILITY = "durability";
	final String EVAL_CURRENT_DATE = "eval-current-date";
	final String EVAL_CURRENT_TIME = "eval-current-time";
	final String EVAL_CURRENT_TIMESTAMP = "eval-current-timestamp";
	final String EVAL_RAND = "eval-rand";
	final String FAILURE_DETECT_SCHEDULE = "failure-detect-schedule";
	final String ID = "id";
	final String INPUT_SINK = "input-sink";
	@Deprecated final String LOCAL = "local";
	final String LOCALITY = "locality";
	final String LOCATION = "location";
	final String LOCK = "lock";
	final String META_DATA_CACHE = "meta-data-cache";
	final String NAME = "name";
	final String PASSWORD = "password";
	final String PROPERTY = "property";
	final String STATE = "state";
	final String SYNC = "sync";
	final String TRANSACTION_MODE = "transaction-mode";
	final String USER = "user";
	final String WEIGHT = "weight";
}
