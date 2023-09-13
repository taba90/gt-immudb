package org.geotools.immudb.aes;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class AESManager {

    private SecretKey secretKey;

    private IvParameterSpec ivParameterSpec;

    private static final String ALG = "AES/CBC/PKCS5Padding";
    private static final String AES="AES";

    public AESManager(String base64Key, String ivParameterSpec){
        byte[] decodedKey = Base64.getDecoder().decode(base64Key);
        this.ivParameterSpec=new IvParameterSpec(ivParameterSpec.getBytes());
        this.secretKey= new SecretKeySpec(decodedKey, 0, decodedKey.length, AES);
    }

    public byte[] encrypt(String algorithm, byte[] input) throws NoSuchPaddingException, NoSuchAlgorithmException,
            InvalidAlgorithmParameterException, InvalidKeyException,
            BadPaddingException, IllegalBlockSizeException {

        Cipher cipher = Cipher.getInstance(algorithm);
        cipher.init(Cipher.ENCRYPT_MODE, secretKey, ivParameterSpec);
        byte[] cipherText = cipher.doFinal(input);
        return cipherText;
    }

    public byte[] encrypt(byte[] input) throws NoSuchPaddingException, NoSuchAlgorithmException,
            InvalidAlgorithmParameterException, InvalidKeyException,
            BadPaddingException, IllegalBlockSizeException {
        return encrypt(ALG,input);

    }

    public byte[] decrypt(byte[] input) throws NoSuchPaddingException, NoSuchAlgorithmException,
            InvalidAlgorithmParameterException, InvalidKeyException,
            BadPaddingException, IllegalBlockSizeException{
        return decrypt(ALG,input);
    }

    public byte[] decrypt(String algorithm, byte[] input) throws NoSuchPaddingException, NoSuchAlgorithmException,
            InvalidAlgorithmParameterException, InvalidKeyException,
            BadPaddingException, IllegalBlockSizeException {

        Cipher cipher = Cipher.getInstance(algorithm);
        cipher.init(Cipher.DECRYPT_MODE, secretKey, ivParameterSpec);
        byte[] decripted = cipher.doFinal(Base64.getDecoder()
                .decode(input));
        return decripted;
    }
}
