package com.tal.shunt.util.aes;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class AESManager {
    public static final String TAG = "AESUtils";
    private static final String KEY_ALGORITHM = "AES";
    private static final String STRING_ENCODE = "UTF-8";
    private static final String PADDING_METHOD = "AES/CBC/NoPadding";


    private static final String KEY = "6017557923588462";

    private static final String IV = "8436630902862183";

    public static String encrypt(String value) {
        try {
            IvParameterSpec iv = new IvParameterSpec(IV.getBytes(STRING_ENCODE));
            SecretKeySpec skeySpec = new SecretKeySpec(KEY.getBytes(STRING_ENCODE), KEY_ALGORITHM);

            Cipher cipher = Cipher.getInstance(PADDING_METHOD);

            //padding
            int blockSize = cipher.getBlockSize();
            byte[] dataBytes = value.getBytes();
            int plaintextLength = dataBytes.length;
            if (plaintextLength % blockSize != 0) {
                plaintextLength = plaintextLength + (blockSize - (plaintextLength % blockSize));
            }
            byte[] plantext = new byte[plaintextLength];
            System.arraycopy(dataBytes, 0, plantext, 0, dataBytes.length);

            cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv);
            byte[] encrypted = cipher.doFinal(plantext);
            return Base64.encodeToString(encrypted, Base64.DEFAULT);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public static String decrypt(String value) {
        try {
            IvParameterSpec iv = new IvParameterSpec(IV.getBytes(STRING_ENCODE));
            SecretKeySpec skeySpec = new SecretKeySpec(KEY.getBytes(STRING_ENCODE), KEY_ALGORITHM);

            Cipher cipher = Cipher.getInstance(PADDING_METHOD);
            cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv);

            byte[] original = cipher.doFinal(Base64.decode(value, Base64.DEFAULT));
            return new String(original, STRING_ENCODE).trim();
        } catch (Exception e) {
            System.out.println("AESManager decrypt error");
        }

        return null;
    }

}
