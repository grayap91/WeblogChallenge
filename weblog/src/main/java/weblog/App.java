package weblog;

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * Created by Gautam on 2017-03-16.
 */
public class App
{
    final static int buffersize = 100;

    public static void main(String[] args) throws Exception
    {
        String fileName = "";
        TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(
                new GzipCompressorInputStream(
                        new BufferedInputStream(
                                new FileInputStream(fileName))));
        tarArchiveInputStream.read();
        byte[] buffer = new byte[100];
        tarArchiveInputStream.read()

    }
}