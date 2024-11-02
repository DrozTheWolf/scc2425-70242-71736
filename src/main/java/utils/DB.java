package utils;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import cloudUtils.HibernateCache;
import utils.Hibernate;
import org.hibernate.Session;

import tukano.api.Result;

public class DB {

	private static final boolean useCache = true;

	// if false then use NoSQL
	private static final boolean usePostegre = true;

	public static <T> List<T> sql(String query, Class<T> clazz) {
		return useCache ? HibernateCache.getInstance().sql(query, clazz) : Hibernate.getInstance().sql(query, clazz);
	}
	
	public static <T> List<T> sql(Class<T> clazz, String fmt, Object ... args) {
		return useCache ? HibernateCache.getInstance().sql(String.format(fmt, args), clazz)
				: Hibernate.getInstance().sql(String.format(fmt, args), clazz);
	}
	
	public static <T> Result<T> getOne(String id, Class<T> clazz) {
		return useCache ? HibernateCache.getInstance().getOne(id, clazz) : Hibernate.getInstance().getOne(id, clazz);
	}
	
	public static <T> Result<T> deleteOne(T obj) {
		return useCache ? HibernateCache.getInstance().deleteOne(obj) : Hibernate.getInstance().deleteOne(obj);
	}
	
	public static <T> Result<T> updateOne(T obj) {
		return useCache ? HibernateCache.getInstance().updateOne(obj) : Hibernate.getInstance().updateOne(obj);
	}
	
	public static <T> Result<T> insertOne( T obj) {
		return useCache ? Result.errorOrValue(HibernateCache.getInstance().persistOne(obj), obj)
				: Result.errorOrValue(Hibernate.getInstance().persistOne(obj), obj);

	}
	
	public static <T> Result<T> transaction( Consumer<Session> c) {
		return useCache ? HibernateCache.getInstance().execute( c::accept ) : Hibernate.getInstance().execute( c::accept );
	}
	
	public static <T> Result<T> transaction( Function<Session, Result<T>> func) {
		return useCache ? HibernateCache.getInstance().execute( func ) : Hibernate.getInstance().execute( func );
	}
}
