package tukano.impl.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Blob;
import java.util.Arrays;
import java.util.function.Consumer;

import cloudUtils.PropsCloud;
import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;

// class for dealing with BLOB operations in Azure
public class CloudBlobIO {

    private final BlobContainerClient contClient;

    // name of the blobs container in azure that is always the same
    private static final String BLOBS_CONTAINER_NAME = "shorts";

    public CloudBlobIO(){

        PropsCloud.load(PropsCloud.PROPS_PATH);

        String infoBlobStorage = PropsCloud.get("BlobStoreConnection", "");

        contClient = new BlobContainerClientBuilder()
                .connectionString(infoBlobStorage)
                .containerName(BLOBS_CONTAINER_NAME)
                .buildClient();
    }

    public boolean blobExists(String blobPath) {
        BlobClient blobClient = contClient.getBlobClient(blobPath);
        return blobClient.exists();
    }

    public void write(String path, byte[] data ) {
        try {
            System.out.println("WRITE>>> " + path);
            BlobClient blobClient = contClient.getBlobClient(path);
            blobClient.upload(BinaryData.fromBytes(data));
        } catch( Exception x ) {
            x.printStackTrace();
        }
    }

    public byte[] read(String path) {
        try {
            System.out.println("READ>>>> " + path);
            BlobClient blobClient = contClient.getBlobClient(path);
            return blobClient.downloadContent().toBytes();
        } catch( Exception x ) {
            x.printStackTrace();
            return null;
        }
    }

    public void read( String path, int chunkSize, Consumer<byte[]> sink) {
        BlobClient blobClient = contClient.getBlobClient(path);
        try (var fis = blobClient.openInputStream()) {
            int n;
            var chunk = new byte[chunkSize];
            while ((n = fis.read(chunk)) > 0)
                sink.accept(Arrays.copyOf(chunk, n));
        } catch (IOException x) {
            throw new RuntimeException(x);
        }
    }

    public boolean delete( String path ) {
        BlobClient blobClient = contClient.getBlobClient(path);
        try {
            if( blobClient.exists() ) {
                blobClient.delete();
                return true;
            }
        } catch( Exception x ) {
            x.printStackTrace();
        }
        return false;
    }


}
