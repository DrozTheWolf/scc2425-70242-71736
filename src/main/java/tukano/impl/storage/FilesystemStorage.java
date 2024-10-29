package tukano.impl.storage;


import static tukano.api.Result.error;
import static tukano.api.Result.ok;
import static tukano.api.Result.ErrorCode.BAD_REQUEST;
import static tukano.api.Result.ErrorCode.CONFLICT;
import static tukano.api.Result.ErrorCode.INTERNAL_ERROR;
import static tukano.api.Result.ErrorCode.NOT_FOUND;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.function.Consumer;

import tukano.api.Result;
import utils.Hash;
import utils.IO;
import tukano.impl.storage.CloudBlobIO;

public class FilesystemStorage implements BlobStorage {
	private final String rootDir;
	private static final int CHUNK_SIZE = 4096;
	private static final String DEFAULT_ROOT_DIR = "/tmp/";

	private final CloudBlobIO cloudIO;

	public FilesystemStorage() {
		this.rootDir = DEFAULT_ROOT_DIR;
		cloudIO = new CloudBlobIO();
	}
	
	@Override
	public Result<Void> write(String path, byte[] bytes) {
		if (path == null)
			return error(BAD_REQUEST);
		;

		if (cloudIO.blobExists(path)) {
			if (Arrays.equals(Hash.sha256(bytes), Hash.sha256(cloudIO.read(path))))
				return ok();
			else
				return error(CONFLICT);

		}
		cloudIO.write(path, bytes);
		return ok();
	}

	@Override
	public Result<byte[]> read(String path) {
		if (path == null)
			return error(BAD_REQUEST);

		if( ! cloudIO.blobExists(path) )
			return error(NOT_FOUND);
		
		var bytes = cloudIO.read(path);
		return bytes != null ? ok( bytes ) : error( INTERNAL_ERROR );
	}

	@Override
	public Result<Void> read(String path, Consumer<byte[]> sink) {
		if (path == null)
			return error(BAD_REQUEST);

		if( ! cloudIO.blobExists(path) )
			return error(NOT_FOUND);
		
		cloudIO.read( path, CHUNK_SIZE, sink );
		return ok();
	}
	
	@Override
	public Result<Void> delete(String path) {
		if (path == null)
			return error(BAD_REQUEST);

        if ( !cloudIO.delete(path)) {
			return error(NOT_FOUND);
		}

        return ok();
	}
	
	private File toFile(String path) {
		var res = new File( rootDir + path );
		
		var parent = res.getParentFile();
		if( ! parent.exists() )
			parent.mkdirs();
		
		return res;
	}

	
}
