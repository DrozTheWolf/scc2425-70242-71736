package cloudUtils;

import java.io.InputStreamReader;
import java.util.Properties;

public class PropsCloud {

	// name of the prop file where the keys are located
	public static final String PROPS_PATH = "azurekeys-region.props";

	public static String get(String key, String defaultValue) {
		var val = System.getProperty(key);
		return val == null ? defaultValue : val;
	}
	
	public static <T> T get(String key, Class<T> clazz) {
		var val = System.getProperty(key);
		if( val == null )
			return null;		
		return utils.JSON.decode(val, clazz);
	}
	
	public static void load( String resourceFile ) {
		try( var in = cloudUtils.PropsCloud.class.getClassLoader().getResourceAsStream(resourceFile) ) {
			var reader = new InputStreamReader(in);
			var props = new Properties();
		    props.load(reader);
			props.forEach( (k,v) -> System.setProperty(k.toString(), v.toString()));
			System.getenv().forEach( System::setProperty );
		}
		catch( Exception x  ) {
			x.printStackTrace();
		}

	}
}
