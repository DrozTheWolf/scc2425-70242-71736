package cloudUtils;

import com.azure.cosmos.*;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import redis.clients.jedis.JedisPool;
import tukano.api.Result;
import org.hibernate.Session;
import tukano.api.Result;
import cloudUtils.PropsCloud;
import cloudUtils.RedisCache;

public class NoSQLCosmosCache {

    private static final String CONTAINER = "users";

    private static cloudUtils.NoSQLCosmosCache instance;

    private CosmosClient client;
    private CosmosDatabase db;
    private CosmosContainer container;

    private static JedisPool pool;

    public static synchronized cloudUtils.NoSQLCosmosCache getInstance() {
        if( instance != null)
            return instance;

        pool = RedisCache.getCachePool();

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
        instance = new cloudUtils.NoSQLCosmosCache(client);
        return instance;
    }

    public NoSQLCosmosCache(CosmosClient client) {
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

    public <T> tukano.api.Result<T> getOne(String id, Class<T> clazz) {
        T resultOBj = RedisCache.getOne(id, clazz, pool);

        if(resultOBj == null){
            var result = tryCatch( () -> container.readItem(id, new PartitionKey(id), clazz).getItem());
            RedisCache.putOne(result.value(), pool);
            return result;
        }
        return Result.ok(resultOBj);
    }

    @SuppressWarnings("unchecked")
    public <T> tukano.api.Result<T> deleteOne(T obj) {
        RedisCache.deleteOne(obj, pool);
        return (tukano.api.Result<T>) tryCatch( () -> container.deleteItem(obj, new CosmosItemRequestOptions()).getItem());
    }

    public <T> tukano.api.Result<T> updateOne(T obj) {
        RedisCache.putOne(obj, pool);
        return tryCatch( () -> container.upsertItem(obj).getItem());
    }

    public <T> tukano.api.Result<T> insertOne(T obj) {
        RedisCache.putOne(obj, pool);
        return tryCatch( () -> container.createItem(obj).getItem());
    }

    public <T> tukano.api.Result<List<T>> query(Class<T> clazz, String queryStr) {
        return tryCatch(() -> {
            var res = container.queryItems(queryStr, new CosmosQueryRequestOptions(), clazz);
            return res.stream().toList();
        });
    }

    public <T> tukano.api.Result<T> executeDB(Consumer<CosmosContainer> proc) {
        return execute(cosmos -> {
            proc.accept(cosmos);
            return tukano.api.Result.ok();
        });
    }

    public <T> tukano.api.Result<T> execute(Function<CosmosContainer, tukano.api.Result<T>> func) {
        try {
            return func.apply(container);
        } catch (CosmosException ce) {
            return tukano.api.Result.error(errorCodeFromStatus(ce.getStatusCode()));
        } catch (Exception e) {
            e.printStackTrace();
            return tukano.api.Result.error(tukano.api.Result.ErrorCode.INTERNAL_ERROR);
        }
    }

    <T> tukano.api.Result<T> tryCatch(Supplier<T> supplierFunc) {
        try {
            init();
            return tukano.api.Result.ok(supplierFunc.get());
        } catch( CosmosException ce ) {
            ce.printStackTrace();
            return tukano.api.Result.error ( errorCodeFromStatus(ce.getStatusCode() ));
        } catch( Exception x ) {
            x.printStackTrace();
            return tukano.api.Result.error( tukano.api.Result.ErrorCode.INTERNAL_ERROR);
        }
    }

    static tukano.api.Result.ErrorCode errorCodeFromStatus(int status ) {
        return switch( status ) {
            case 200 -> tukano.api.Result.ErrorCode.OK;
            case 404 -> tukano.api.Result.ErrorCode.NOT_FOUND;
            case 409 -> tukano.api.Result.ErrorCode.CONFLICT;
            default -> tukano.api.Result.ErrorCode.INTERNAL_ERROR;
        };
    }


}
