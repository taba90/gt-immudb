package org.geotools.immudb;

import io.codenotary.immudb4j.sql.SQLValue;
import org.geotools.data.store.ContentState;
import org.geotools.immudb.aes.AESManager;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.NoSuchElementException;

import static org.geotools.immudb.Converter.toValue;

public class DecryptingImmuDBFeatureReader extends ImmuDBFeatureReader{

   private AESManager aesManager;

    public DecryptingImmuDBFeatureReader(ImmuDBDataStore dataStore, ContentState state, SimpleFeatureType simpleFeatureType, String sql, SQLValue[] params, String privateKey, String iv) throws IOException {
        super(dataStore, state, simpleFeatureType, sql, params);
        this.aesManager=new AESManager(privateKey,iv);
    }

    @Override
    public SimpleFeature next() throws IOException, IllegalArgumentException, NoSuchElementException {
        SimpleFeature sf= super.next();
        List< AttributeDescriptor> ads=simpleFeatureType.getAttributeDescriptors();
        for (int i=0; i<ads.size(); i++){
            AttributeDescriptor ad=ads.get(i);
            Class<?> type=ad.getType().getBinding();
            byte[] encrypted=(byte[])sf.getAttribute(i);
            try {
                byte[] value=aesManager.decrypt(encrypted);
                sf.setAttribute(i,toValue(value,type));
            } catch (NoSuchPaddingException e) {
                throw new RuntimeException(e);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            } catch (InvalidAlgorithmParameterException e) {
                throw new RuntimeException(e);
            } catch (InvalidKeyException e) {
                throw new RuntimeException(e);
            } catch (BadPaddingException e) {
                throw new RuntimeException(e);
            } catch (IllegalBlockSizeException e) {
                throw new RuntimeException(e);
            }
        }
        return sf;
    }
}
