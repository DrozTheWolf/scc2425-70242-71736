package cloudUtils;

import com.azure.cosmos.*;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import org.hibernate.Session;
import tukano.api.Result;
import cloudUtils.PropsCloud;

public class NoSQLCosmos {

    private static final String CONTAINER = "users";

    private static NoSQLCosmos instance;

    private CosmosClient client;
    private CosmosDatabase db;
    private CosmosContainer container;

    public static synchronized NoSQLCosmos getInstance() {
        if( instance != null)
            return instance;

        PropsCloud.load(PropsCloud.PROPS_PATH);
        String connect_url = PropsCloud.get("COSMOSDB_URL", "");
        String connect_key = PropsCloud.get("COSMOSDB_KEY", "");

        CosmosClient client = new CosmosClientBuilder()
                .endpoint(connect_url)
                .key(connect_key)
                //.directMode()
                .gatewayMode()
                // replace by .directMode() for better performance
                .consistencyLevel(ConsistencyLevel.SESSION)
                .connectionSharingAcrossClientsEnabled(true)
                .contentResponseOnWriteEnabled(true)
                .buildClient();
        instance = new NoSQLCosmos(client);
        return instance;
    }

    public NoSQLCosmos(CosmosClient client) {
        this.client = client;
    }

    private synchronized void init() {
        if( db != null)
            return;

        PropsCloud.load(PropsCloud.PROPS_PATH);
        String connect_db = PropsCloud.get("COSMOSDB_DATABASE", "");

        db = client.getDatabase(connect_db);
        container = db.getContainer(CONTAINER);
    }

    public void close() {
        client.close();
    }

    public <T> Result<T> getOne(String id, Class<T> clazz) {
        return tryCatch( () -> container.readItem(id, new PartitionKey(id), clazz).getItem());
    }

    @SuppressWarnings("unchecked")
    public <T> Result<T> deleteOne(T obj) {
        return (Result<T>) tryCatch( () -> container.deleteItem(obj, new CosmosItemRequestOptions()).getItem());
    }

    public <T> Result<T> updateOne(T obj) {
        return tryCatch( () -> container.upsertItem(obj).getItem());
    }

    public <T> Result<T> insertOne( T obj) {
        return tryCatch( () -> container.createItem(obj).getItem());
    }

    public <T> Result<List<T>> query(Class<T> clazz, String queryStr) {
        return tryCatch(() -> {
            var res = container.queryItems(queryStr, new CosmosQueryRequestOptions(), clazz);
            return res.stream().toList();
        });
    }

    public <T> Result<T> executeDB(Consumer<CosmosContainer> proc) {
        return execute(cosmos -> {
            proc.accept(cosmos);
            return Result.ok();
        });
    }

    public <T> Result<T> execute(Function<CosmosContainer, Result<T>> func) {
        try {
            return func.apply(container);
        } catch (CosmosException ce) {
            return Result.error(errorCodeFromStatus(ce.getStatusCode()));
        } catch (Exception e) {
            e.printStackTrace();
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }
    }

    <T> Result<T> tryCatch( Supplier<T> supplierFunc) {
        try {
            init();
            return Result.ok(supplierFunc.get());
        } catch( CosmosException ce ) {
            ce.printStackTrace();
            return Result.error ( errorCodeFromStatus(ce.getStatusCode() ));
        } catch( Exception x ) {
            x.printStackTrace();
            return Result.error( Result.ErrorCode.INTERNAL_ERROR);
        }
    }

    static Result.ErrorCode errorCodeFromStatus( int status ) {
        return switch( status ) {
            case 200 -> Result.ErrorCode.OK;
            case 404 -> Result.ErrorCode.NOT_FOUND;
            case 409 -> Result.ErrorCode.CONFLICT;
            default -> Result.ErrorCode.INTERNAL_ERROR;
        };
    }


}
