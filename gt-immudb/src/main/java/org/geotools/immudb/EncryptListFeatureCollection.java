package org.geotools.immudb;

import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.collection.DelegateSimpleFeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.immudb.aes.AESManager;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.GeometryDescriptor;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class EncryptListFeatureCollection extends ListFeatureCollection {

    protected AESManager aesManager;

    private static final String ENCRYPTED="ENCRYPTED";

    public EncryptListFeatureCollection(String secretKey, String ivParameterSpec, SimpleFeatureType schema, List<SimpleFeature> list) {
        super(schema, list);
        this.aesManager=new AESManager(secretKey,ivParameterSpec);
    }

    public EncryptListFeatureCollection(String  secretKey, String ivParameterSpec, SimpleFeatureType schema, SimpleFeature... array) {
        super(schema, array);
        this.aesManager=new AESManager(secretKey,ivParameterSpec);
    }

    public EncryptListFeatureCollection(String secretKey, String ivParameterSpec, SimpleFeatureCollection copy) throws IOException {
        super(copy);
        this.aesManager=new AESManager(secretKey,ivParameterSpec);
    }

    @Override
    public boolean add(SimpleFeature f) {
        return super.add(f);
    }

    @Override
    public SimpleFeatureIterator features() {
        return new EncryptFeatureIterator(list.iterator(),schema);
    }

    @Override
    protected Iterator<SimpleFeature> openIterator() {
        return new EncryptFeatureIterator(list.iterator(),schema);
    }

    class EncryptFeatureIterator implements SimpleFeatureIterator, Iterator<SimpleFeature> {

        private Iterator<SimpleFeature> iterator;

        private SimpleFeatureType schema;

        private SimpleFeatureType oldSchema;
        /**
         * Wrap the provided iterator up as a FeatureIterator.
         *
         * @param iterator Iterator to be used as a delegate.
         */
        public EncryptFeatureIterator(Iterator<SimpleFeature> iterator, SimpleFeatureType schema) {
            this.schema=Converter.toByteArrayType(schema);
            this.oldSchema=schema;
            this.iterator=iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public SimpleFeature next() {
            return encode(iterator.next());
        }

        @Override
        public void remove() {
            iterator.remove();
        }

        @Override
        public void forEachRemaining(Consumer<? super SimpleFeature> action) {
            iterator.forEachRemaining(action);
        }

        @Override
        public void close() {

        }
        SimpleFeature encode(SimpleFeature  f){
            if (!alreadyEncrypted(f)) {
                SimpleFeatureBuilder sfb=new SimpleFeatureBuilder(schema);
                List<AttributeDescriptor> ads = oldSchema.getAttributeDescriptors();
                for (int i = 0; i < ads.size(); i++) {
                    Object value = f.getAttribute(i);
                    Class<?> type = ads.get(i).getType().getBinding();
                    byte[] bytes = Converter.toByteArray(value, type);
                    try {
                       sfb.set(i, bytes != null ? aesManager.encrypt(bytes) : null);
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
                f=sfb.buildFeature(null);
                f.getUserData().put(ENCRYPTED,true);
            }
            return f;
        }

        private boolean alreadyEncrypted(SimpleFeature sf){
            boolean result=false;
            Map<Object,Object> userdata=sf.getUserData();
            if (userdata.containsKey(ENCRYPTED)){
                Object val=userdata.get(ENCRYPTED);
                if (val!=null && val instanceof Boolean)
                    result=((Boolean) val).booleanValue();
            }
            return result;
        }
    }




}
