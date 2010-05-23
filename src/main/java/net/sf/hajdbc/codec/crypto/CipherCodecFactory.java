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
package net.sf.hajdbc.codec.crypto;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.sql.SQLException;
import java.util.Properties;

import net.sf.hajdbc.codec.Codec;
import net.sf.hajdbc.codec.CodecFactory;
import net.sf.hajdbc.sql.SQLExceptionFactory;
import net.sf.hajdbc.util.Strings;

/**
 * Used to decrypt configuration file passwords using a symmetric key stored in a keystore.
 * <table>
 * 	<tr>
 * 		<th>Property</th>
 * 		<th>Default</th>
 * 	</tr>
 * 	<tr>
 * 		<td>ha-jdbc.keystore.file</td>
 * 		<td>~/.keystore</td>
 * 	</tr>
 * 	<tr>
 * 		<td>ha-jdbc.keystore.type</td>
 * 		<td>jks</td>
 * 	</tr>
 * 	<tr>
 * 		<td>ha-jdbc.keystore.password</td>
 * 		<td><em>none</em></td>
 * 	</tr>
 * 	<tr>
 * 		<td>ha-jdbc.key.alias</td>
 * 		<td>ha-jdbc</td>
 * 	</tr>
 * 	<tr>
 * 		<td>ha-jdbc.key.password</td>
 * 		<td><em>required</em><td>
 * 	</tr>
 * </table>
 * @author Paul Ferraro
 */
public class CipherCodecFactory implements CodecFactory, Serializable
{
	private static final long serialVersionUID = -4409167180573651279L;
	
	public static final String DEFAULT_KEYSTORE_FILE = String.format("%s/.keystore", System.getProperty("user.home"));
	public static final String DEFAULT_KEY_ALIAS = "ha-jdbc";
	
	private static final String KEYSTORE_FILE = "ha-jdbc.keystore.file";
	private static final String KEYSTORE_TYPE = "ha-jdbc.keystore.type";
	private static final String KEYSTORE_PASSWORD = "ha-jdbc.keystore.password";
	private static final String KEY_ALIAS = "ha-jdbc.key.alias";
	private static final String KEY_PASSWORD = "ha-jdbc.key.password";
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.codec.CodecFactory#createDecoder(java.util.Properties)
	 */
	@Override
	public Codec createDecoder(Properties properties) throws SQLException
	{
		String type = properties.getProperty(KEYSTORE_TYPE, KeyStore.getDefaultType());
		File file = new File(properties.getProperty(KEYSTORE_FILE, DEFAULT_KEYSTORE_FILE));
		String password = properties.getProperty(KEYSTORE_PASSWORD);

		String keyAlias = properties.getProperty(KEY_ALIAS, DEFAULT_KEY_ALIAS);
		String keyPassword = properties.getProperty(KEY_PASSWORD);
		
		try
		{
			KeyStore store = KeyStore.getInstance(type);
			
			InputStream input = new FileInputStream(file);
			
			try
			{
				store.load(input, (password != null) ? password.toCharArray() : null);
			}
			finally
			{
				input.close();
			}

			return new CipherCodec(store.getKey(keyAlias, (keyPassword != null) ? keyPassword.toCharArray() : null));
		}
		catch (GeneralSecurityException e)
		{
			throw SQLExceptionFactory.getInstance().createException(e);
		}
		catch (IOException e)
		{
			throw SQLExceptionFactory.getInstance().createException(e);
		}
	}
	
	public static void main(String... args)
	{
		if (args.length != 1)
		{
			System.err.println(String.format("Usage:%s\tjava %s <password-to-encrypt>", Strings.NEW_LINE, CipherCodecFactory.class.getName()));
			System.exit(1);
			return;
		}
		
		String value = args[0];
		
		try
		{
			Codec codec = new CipherCodecFactory().createDecoder(System.getProperties());

			System.out.println(codec.encode(value));
		}
		catch (SQLException e)
		{
			e.printStackTrace(System.err);
		}
	}
}
