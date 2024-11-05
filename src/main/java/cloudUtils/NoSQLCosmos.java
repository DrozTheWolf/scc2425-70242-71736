package cloudUtils;

import com.azure.cosmos.*;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import org.hibernate.Session;
import tukano.api.Result;
import cloudUtils.PropsCloud;

import tukano.api.User;
import tukano.api.Short;
import tukano.impl.data.Following;
import tukano.impl.data.Likes;

public class NoSQLCosmos {

    private static final String CONTAINER_USERS = "users";
    private static final String CONTAINER_SHORTS = "shorts";
    private static final String CONTAINER_FOLLOW = "followers";
    private static final String CONTAINER_LIKES = "likes";

    private static final String KEY_PATH = "/id"; // Adjust as needed

    private static final int NOT_FOUND = 404;

    private static NoSQLCosmos instance;

    private CosmosClient client;
    private static CosmosDatabase db;

    public static synchronized NoSQLCosmos getInstance() {
        if( instance != null)
            return instance;

        PropsCloud.load(PropsCloud.PROPS_PATH);
        String connect_url = PropsCloud.get("COSMOSDB_URL", "");
        String connect_key = PropsCloud.get("COSMOSDB_KEY", "");
        String connect_db = PropsCloud.get("COSMOSDB_DATABASE", "");

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
        db = client.getDatabase(connect_db);

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
    }

    private <T> CosmosContainer getCosmosContainer(Class<T> clazz){

        if (clazz == User.class){
            return createOrReturnContainer(CONTAINER_USERS);
        } else if (clazz == Short.class){
            return createOrReturnContainer(CONTAINER_SHORTS);
        } else if (clazz == Following.class){
            return createOrReturnContainer(CONTAINER_FOLLOW);
        } else {
            return createOrReturnContainer(CONTAINER_LIKES);
        }
    }

    private CosmosContainer createOrReturnContainer(String name){
        try {
            CosmosContainer container = db.getContainer(name);
            container.read();
            return container;
        } catch (CosmosException e){
            if (e.getStatusCode() == NOT_FOUND){
                CosmosContainerProperties containerProps = new CosmosContainerProperties(name, KEY_PATH);
                db.createContainer(containerProps);
                return db.getContainer(name);
            }
            throw e;
        }
    }

    public void close() {
        client.close();
    }

    public <T> Result<T> getOne(String id, Class<T> clazz) {
        CosmosContainer container = getCosmosContainer(clazz);
        return tryCatch( () -> container.readItem(id, new PartitionKey(id), clazz).getItem());
    }

    @SuppressWarnings("unchecked")
    public <T> Result<T> deleteOne(T obj) {
        CosmosContainer container = getCosmosContainer(obj.getClass());
        return (Result<T>) tryCatch( () -> container.deleteItem(obj, new CosmosItemRequestOptions()).getItem());
    }

    public <T> Result<T> updateOne(T obj) {
        CosmosContainer container = getCosmosContainer(obj.getClass());
        return tryCatch( () -> container.upsertItem(obj).getItem());
    }

    public <T> Result<T> insertOne( T obj) {
        CosmosContainer container = getCosmosContainer(obj.getClass());
        return tryCatch( () -> container.createItem(obj).getItem());
    }

    public <T, Q> Result<List<T>> query(Class<T> clazz, String queryStr, Class<Q> seeFrom) {
        CosmosContainer container = getCosmosContainer(seeFrom);
        return tryCatch(() -> {
            var res = container.queryItems(queryStr, new CosmosQueryRequestOptions(), clazz);
            return res.stream().toList();
        });
    }

    <T> Result<T> tryCatch( Supplier<T> supplierFunc) {
        try {
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
