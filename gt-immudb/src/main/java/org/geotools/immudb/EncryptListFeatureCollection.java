package org.geotools.immudb;

import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.collection.DelegateSimpleFeatureIterator;
import org.geotools.immudb.aes.AESManager;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class EncryptListFeatureCollection extends ListFeatureCollection {

    protected AESManager aesManager;

    public EncryptListFeatureCollection(String secretKey, String ivParameterSpec, SimpleFeatureType schema, List<SimpleFeature> list) {
        super(schema, list);
        this.aesManager=new AESManager(secretKey,ivParameterSpec);
        this.list=list.stream().map(f->encode(f)).collect(Collectors.toList());
    }

    public EncryptListFeatureCollection(String  secretKey, String ivParameterSpec, SimpleFeatureType schema, SimpleFeature... array) {
        super(schema, array);
        this.aesManager=new AESManager(secretKey,ivParameterSpec);
        this.list=list.stream().map(f->encode(f)).collect(Collectors.toList());
    }

    public EncryptListFeatureCollection(String secretKey, String ivParameterSpec, SimpleFeatureCollection copy) throws IOException {
        super(copy);
        this.aesManager=new AESManager(secretKey,ivParameterSpec);
        this.list=list.stream().map(f->encode(f)).collect(Collectors.toList());
    }

    @Override
    public boolean add(SimpleFeature f) {
        f=encode(f);
        return super.add(f);
    }

    SimpleFeature encode(SimpleFeature  f){
        List<AttributeDescriptor> ads=schema.getAttributeDescriptors();
        for (int i=0; i<ads.size(); i++){
            Object value=f.getAttribute(i);
            Class<?> type=ads.get(i).getType().getBinding();
            byte[] bytes=Converter.toByteArray(value,type);
            try {
                f.setAttribute(i,aesManager.encrypt(bytes));
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
        return f;
    }

    @Override
    protected Iterator<SimpleFeature> openIterator() {
        return new EncryptFeatureIterator(list.iterator());
    }

    class EncryptFeatureIterator implements SimpleFeatureIterator, Iterator<SimpleFeature> {

        private Iterator<SimpleFeature> iterator;
        /**
         * Wrap the provided iterator up as a FeatureIterator.
         *
         * @param iterator Iterator to be used as a delegate.
         */
        public EncryptFeatureIterator(Iterator<SimpleFeature> iterator) {
            this.iterator=iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public SimpleFeature next() {
            return EncryptListFeatureCollection.this.encode(iterator.next());
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
    }


}
