/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2014  Paul Ferraro
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
package net.sf.hajdbc.xml;

import java.net.URL;

import javax.xml.XMLConstants;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.xml.sax.SAXException;

public enum Namespace
{
	VERSION_3_0(3, 0, DatabaseClusterConfigurationReader_3_0.FACTORY),
	VERSION_3_1(3, 1, DatabaseClusterConfigurationReader_3_1.FACTORY),
	;

	public static final Namespace CURRENT_VERSION = VERSION_3_0;

	private final int major;
	private final int minor;
	private final Schema schema;
	private final DatabaseClusterConfigurationReaderFactory factory;

	private Namespace(int major, int minor, DatabaseClusterConfigurationReaderFactory factory) {
		this.major = major;
		this.minor = minor;
		this.factory = factory;
		
		String resource = String.format("ha-jdbc-%d.%d.xsd", major, minor);
		URL url = this.getClass().getClassLoader().getResource(resource);

		if (url == null) {
			throw new IllegalArgumentException(resource);
		}
		try {
			this.schema = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI).newSchema(url);
		} catch (SAXException e) {
			throw new IllegalStateException(e);
		}
	}

	public Schema getSchema() {
		return this.schema;
	}
	public String getURI() {
		return String.format("urn:ha-jdbc:cluster:%d.%d", this.major, this.minor);
	}

	public DatabaseClusterConfigurationReaderFactory getReaderFactory() {
		return this.factory;
	}
}
