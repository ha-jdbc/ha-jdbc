/*
 * HA-JDBC: High-Availablity JDBC
 * Copyright 2011 Paul Ferraro
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
import java.io.FileOutputStream;
import java.security.Key;
import java.security.KeyStore;
import java.sql.SQLException;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;

import net.sf.hajdbc.codec.Codec;
import net.sf.hajdbc.util.Resources;

import org.apache.commons.codec.binary.Base64;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Paul Ferraro
 */
public class CipherCodecFactoryTest
{
	private static final String KEY = "x4zOovfg7+Y=";
	private static final String ALGORITHM = "DES";
	private static final String STORE_PASSWORD = "password_store";
	private static final String KEY_PASSWORD = "password_key";
	
	private Key key;
	
	@Before
	public void before() throws Exception
	{
		File file = File.createTempFile("ha-jdbc", "keystore");
		
		SecretKeyFactory factory = SecretKeyFactory.getInstance(ALGORITHM);
		this.key = factory.generateSecret(new DESKeySpec(Base64.decodeBase64(KEY.getBytes())));
		KeyStore store = KeyStore.getInstance(CipherCodecFactory.Property.KEYSTORE_TYPE.defaultValue);
		store.load(null, null);
		store.setKeyEntry(CipherCodecFactory.Property.KEY_ALIAS.defaultValue, this.key, KEY_PASSWORD.toCharArray(), null);
		
		FileOutputStream out = new FileOutputStream(file);
		try
		{
			store.store(out, STORE_PASSWORD.toCharArray());
		}
		finally
		{
			Resources.close(out);
		}
		
		System.setProperty(CipherCodecFactory.Property.KEYSTORE_FILE.name, file.getPath());
		System.setProperty(CipherCodecFactory.Property.KEYSTORE_PASSWORD.name, STORE_PASSWORD);
		System.setProperty(CipherCodecFactory.Property.KEY_PASSWORD.name, KEY_PASSWORD);
	}
	
	@Test
	public void test() throws SQLException
	{
		Codec codec = new CipherCodecFactory().createCodec("cluster");
		
		Assert.assertTrue(codec instanceof CipherCodec);
		Assert.assertEquals(this.key, ((CipherCodec) codec).getKey());
	}
}
