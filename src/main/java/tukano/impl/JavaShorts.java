package tukano.impl;

import static java.lang.String.format;
import static tukano.api.Result.error;
import static tukano.api.Result.errorOrResult;
import static tukano.api.Result.errorOrValue;
import static tukano.api.Result.errorOrVoid;
import static tukano.api.Result.ok;
import static tukano.api.Result.ErrorCode.BAD_REQUEST;
import static tukano.api.Result.ErrorCode.FORBIDDEN;
import static utils.DB.getOne;
import static utils.DB.sqlDB;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

import tukano.api.Blobs;
import tukano.api.Result;
import tukano.api.Short;
import tukano.api.Shorts;
import tukano.api.User;
import tukano.impl.data.Following;
import tukano.impl.data.Likes;
import tukano.impl.rest.TukanoRestServer;
import utils.DB;

public class JavaShorts implements Shorts {

	private static Logger Log = Logger.getLogger(JavaShorts.class.getName());
	
	private static Shorts instance;
	
	synchronized public static Shorts getInstance() {
		if( instance == null )
			instance = new JavaShorts();
		return instance;
	}
	
	private JavaShorts() {}
	
	
	@Override
	public Result<Short> createShort(String userId, String password) {
		Log.info(() -> format("createShort : userId = %s, pwd = %s\n", userId, password));

		return errorOrResult( okUser(userId, password), user -> {
			
			var shortId = format("%s+%s", userId, UUID.randomUUID());
			var blobUrl = format("%s/%s/%s", TukanoRestServer.serverURI, Blobs.NAME, shortId); 
			var shrt = new Short(shortId, userId, blobUrl);

			return errorOrValue(DB.insertOne(shrt), s -> s.copyWithLikes_And_Token(0));
		});
	}

	@Override
	public Result<Short> getShort(String shortId) {
		Log.info(() -> format("getShort : shortId = %s\n", shortId));

		if( shortId == null )
			return error(BAD_REQUEST);

		List<Long> likes;
		if(DB.usePostegre){
			var query = format("SELECT count(*) FROM Likes l WHERE l.shortId = '%s'", shortId);
			likes = DB.sql(query, Long.class);
		} else {
			var query = format("SELECT VALUE COUNT(1) FROM l WHERE l.shortId = '%s'", shortId);
			likes = DB.sqlDB(query, Long.class, Likes.class);
		}

		return errorOrValue( getOne(shortId, Short.class), shrt -> shrt.copyWithLikes_And_Token( likes.get(0)));
	}

	
	@Override
	public Result<Void> deleteShort(String shortId, String password) {
		Log.info(() -> format("deleteShort : shortId = %s, pwd = %s\n", shortId, password));

		if(DB.usePostegre){
			return errorOrResult( getShort(shortId), shrt -> {

				return errorOrResult( okUser( shrt.getOwnerId(), password), user -> {
					return DB.transaction( hibernate -> {

						hibernate.remove( shrt);

						var query = format("DELETE FROM Likes WHERE shortId = '%s'", shortId);

						hibernate.createNativeQuery( query, Likes.class).executeUpdate();

						JavaBlobs.getInstance().delete(shrt.getBlobUrl(), Token.get() );
					});
				});
			});
		} else {
			return errorOrResult( getShort(shortId), shrt -> {

				DB.deleteOne(shrt);

				String query = format("SELECT * FROM l WHERE l.shortId = '%s'", shortId);
				List<Map> likesList = DB.sqlDB(query, Map.class, Likes.class);

				for (Map likeData : likesList) {
					Likes like = new Likes();
					like.setLikesId((String) likeData.get("id"));
					DB.deleteOne(like);
				}

				tukano.impl.JavaBlobs.getInstance().delete(shrt.getBlobUrl(), tukano.impl.Token.get() );
                return ok();
            });
		}
	}

	@Override
	public Result<List<String>> getShorts(String userId) {
		Log.info(() -> format("getShorts : userId = %s\n", userId));

		if (DB.usePostegre){
			String query = format("SELECT s.shortId FROM Short s WHERE s.ownerId = '%s'", userId);
			return errorOrValue( okUser(userId), DB.sql( query, String.class));
		} else {
			String query = format("SELECT s.id FROM s WHERE s.ownerId = '%s'", userId);
			var result = DB.sqlDB( query, Map.class, Short.class);

			List<String> resultsList = result.stream()
					.map(map -> (String) map.get("id"))
					.toList();

			return errorOrValue( okUser(userId), resultsList);
		}
	}

	@Override
	public Result<Void> follow(String userId1, String userId2, boolean isFollowing, String password) {
		Log.info(() -> format("follow : userId1 = %s, userId2 = %s, isFollowing = %s, pwd = %s\n", userId1, userId2, isFollowing, password));
	
		
		return errorOrResult( okUser(userId1, password), user -> {
			var f = new Following(userId1, userId2);
			return errorOrVoid( okUser( userId2), isFollowing ? DB.insertOne( f ) : DB.deleteOne( f ));	
		});			
	}

	@Override
	public Result<List<String>> followers(String userId, String password) {
		Log.info(() -> format("followers : userId = %s, pwd = %s\n", userId, password));

		if (DB.usePostegre){
			var query = format("SELECT f.follower FROM Following f WHERE f.followee = '%s'", userId);
			return errorOrValue( okUser(userId, password), DB.sql(query, String.class));
		} else {
			var query = format("SELECT f.follower FROM f WHERE f.followee = '%s'", userId);
			var result = DB.sqlDB( query, Map.class, Following.class);

			List<String> resultsList = result.stream()
					.map(map -> (String) map.get("follower"))
					.toList();

			return errorOrValue( okUser(userId, password), resultsList);
		}

	}

	@Override
	public Result<Void> like(String shortId, String userId, boolean isLiked, String password) {
		Log.info(() -> format("like : shortId = %s, userId = %s, isLiked = %s, pwd = %s\n", shortId, userId, isLiked, password));

		
		return errorOrResult( getShort(shortId), shrt -> {
			var l = new Likes(userId, shortId, shrt.getOwnerId());
			return errorOrVoid( okUser( userId, password), isLiked ? DB.insertOne( l ) : DB.deleteOne( l ));	
		});
	}

	@Override
	public Result<List<String>> likes(String shortId, String password) {
		Log.info(() -> format("likes : shortId = %s, pwd = %s\n", shortId, password));

		if (DB.usePostegre){
			return errorOrResult( getShort(shortId), shrt -> {

				var query = format("SELECT l.userId FROM Likes l WHERE l.shortId = '%s'", shortId);

				return errorOrValue( okUser( shrt.getOwnerId(), password ), DB.sql(query, String.class));
			});
		} else {
			return errorOrResult( getShort(shortId), shrt -> {

				var query = format("SELECT l.userId FROM l WHERE l.shortId = '%s'", shortId);
				var result = DB.sqlDB(query, Map.class, Likes.class);

				List<String> resultsList = result.stream()
						.map(map -> (String) map.get("userId"))
						.toList();

				return errorOrValue( okUser( shrt.getOwnerId(), password ), resultsList);
			});
		}

	}

	@Override
	public Result<List<String>> getFeed(String userId, String password) {
		Log.info(() -> format("getFeed : userId = %s, pwd = %s\n", userId, password));

		final var QUERY_FMT = """
				SELECT s.shortId, s.timestamp FROM Short s WHERE	s.ownerId = '%s'				
				UNION			
				SELECT s.shortId, s.timestamp FROM Short s, Following f 
					WHERE 
						f.followee = s.ownerId AND f.follower = '%s' 
				ORDER BY s.timestamp DESC""";

		return errorOrValue( okUser( userId, password), DB.sql( format(QUERY_FMT, userId, userId), String.class));		
	}
		
	protected Result<User> okUser( String userId, String pwd) {
		return JavaUsers.getInstance().getUser(userId, pwd);
	}
	
	private Result<Void> okUser( String userId ) {
		var res = okUser( userId, "");
		if( res.error() == FORBIDDEN )
			return ok();
		else
			return error( res.error() );
	}
	
	@Override
	public Result<Void> deleteAllShorts(String userId, String password, String token) {
		Log.info(() -> format("deleteAllShorts : userId = %s, password = %s, token = %s\n", userId, password, token));

		if( ! Token.isValid( token, userId ) )
			return error(FORBIDDEN);

		if (DB.usePostegre){
			// transaction should be changed if we are using NoSQL
			return DB.transaction( (hibernate) -> {

				//delete shorts
				var query1 = format("DELETE FROM Short WHERE ownerid = '%s'", userId);
				hibernate.createNativeQuery(query1, Short.class).executeUpdate();

				//delete follows
				var query2 = format("DELETE FROM Following WHERE follower = '%s' OR followee = '%s'", userId, userId);
				hibernate.createNativeQuery(query2, Following.class).executeUpdate();

				//delete likes
				var query3 = format("DELETE FROM Likes WHERE ownerid = '%s' OR userid = '%s'", userId, userId);
				hibernate.createNativeQuery(query3, Likes.class).executeUpdate();

			});
		} else {

			var queryShorts = format("SELECT * FROM s WHERE s.ownerId = '%s'", userId);
			List<Map> shortsToDelete = sqlDB(queryShorts, Map.class, Short.class);

			for (Map shortsData : shortsToDelete) {
				Short st = new Short();
				st.setShortId((String) shortsData.get("id"));
				DB.deleteOne(st);
			}

			var queryFollow = format("SELECT * FROM f WHERE f.follower = '%s' OR f.followee = '%s'", userId, userId);
			List<Map> followToDelete = sqlDB(queryFollow, Map.class, Following.class);

			for (Map followData : followToDelete) {
				Following fll = new Following();
				fll.setFollowingId((String) followData.get("id"));
				DB.deleteOne(fll);
			}

			var queryLikes = format("SELECT * FROM l WHERE l.ownerId = '%s' OR l.userId = '%s'", userId, userId);
			List<Map> likesList = DB.sqlDB(queryLikes, Map.class, Likes.class);

			for (Map likeData : likesList) {
				Likes like = new Likes();
				like.setLikesId((String) likeData.get("id"));
				DB.deleteOne(like);
			}

			return ok();
		}
	}
}