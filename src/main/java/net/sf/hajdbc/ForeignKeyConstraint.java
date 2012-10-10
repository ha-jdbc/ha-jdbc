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
package net.sf.hajdbc;

import java.util.List;




/**
 * Represents a foreign key constraint on a table.
 * 
 * @author  Paul Ferraro
 * @since   1.1
 */
public interface ForeignKeyConstraint extends UniqueConstraint
{
	/**
	 * @return the foreign table of this foreign key
	 */
	QualifiedName getForeignTable();
	
	/**
	 * @return the foreign column of this foreign key
	 */
	List<String> getForeignColumnList();
	
	/**
	 * @return Returns the deleteRule.
	 */
	int getDeleteRule();

	/**
	 * @return Returns the updateRule.
	 */
	int getUpdateRule();

	/**
	 * @return Returns the deferrability.
	 */
	int getDeferrability();

	/**
	 * @param deferrability The deferrability to set.
	 */
	void setDeferrability(int deferrability);

	/**
	 * @param deleteRule The deleteRule to set.
	 */
	void setDeleteRule(int deleteRule);

	/**
	 * @param foreignTable The foreignTable to set.
	 */
	void setForeignTable(QualifiedName foreignTable);

	/**
	 * @param updateRule The updateRule to set.
	 */
	void setUpdateRule(int updateRule);
}
