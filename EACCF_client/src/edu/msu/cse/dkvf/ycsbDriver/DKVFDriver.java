package edu.msu.cse.dkvf.ycsbDriver;

import com.yahoo.ycsb.*;
import edu.msu.cse.dkvf.DKVFClient;
import edu.msu.cse.dkvf.config.ConfigReader;
import org.apache.htrace.core.Tracer;

import java.lang.reflect.Constructor;
import java.text.MessageFormat;
import java.util.*;

/**
 * The YCSB driver for DKVF. 
 *
 */
public class DKVFDriver extends DB {
	public DKVFClient client;

    /**
	 * Initializes the driver.
	 * @throws DBException
	 */
	public void init() throws DBException {
		Properties p = getProperties();
		Object configObject = p.get("clientConfigFile");
		if (configObject == null)
			throw new DBException("No client config file found in properties");
		String configFileAddress = (String) configObject;
		System.out.println(MessageFormat.format("Client config file: {0}", configFileAddress));

		ConfigReader cnfReader = null;
		try {
			cnfReader = new ConfigReader(configFileAddress);
		} catch (Exception e) {
			throw new DBException("Error in reading config file.");
		}
		String clientClassName = (String) p.getProperty("clientClassName");
		System.out.println(MessageFormat.format("client class name: {0}", clientClassName));
		runClient(cnfReader, clientClassName);
	}

	/**
	 * Runs the YCSB client. 
	 * @param cnfReader The configuration reader object. 
	 * @param clientClassName The name of the client class. It is the client side of key-value written by DKVF.
	 * @throws DBException
	 */
	private void runClient(ConfigReader cnfReader, String clientClassName) throws DBException {
		try {
			Class<?> clazz = Class.forName(clientClassName);
			Constructor<?> ctor = clazz.getConstructor(ConfigReader.class);
			DKVFClient client = (DKVFClient) ctor.newInstance(new Object[] { cnfReader });
			client.runAll();
			this.client = client;
		} catch (Exception e) {
			e.printStackTrace();
			throw new DBException("problem in instantiating client object:");

		}
	}

	//--------DKVF workload generator does not call default YCSB functions.
	@Override
	// No delete in DKVF!
	public Status delete(String arg0, String arg1) {
		return Status.FORBIDDEN;
	}


	@Override
	// No scan in DKVF!
	public Status scan(String arg0, String arg1, int arg2, Set<String> arg3, Vector<HashMap<String, ByteIterator>> arg4) {
		return Status.FORBIDDEN;
	}

	@Override
	public Status insert(String table, String key, Map<String, ByteIterator> values) {
		ByteIterator value = values.get(key);
		if (value == null) {
			return Status.FORBIDDEN;
		} else {
			return insert(key, value);
		}
	}

	@Override
	public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {

	    byte[] _result = client.get(key);
        if (result == null) return Status.NOT_FOUND;

        result.put(key, new ByteArrayByteIterator(_result));
        return Status.OK;
	}
	
	@Override
	public Status update(String arg0, String arg1, Map<String, ByteIterator> arg2) {
		return insert(arg0, arg1, arg2);
	}
	//-------------------

	//----- DKVF workload calls the following functions-----------------
	
	/**
	 * Updates the value. The same functionality as insert. 
	 * @param key
	 * @param value
	 * @return The result of the operation
	 */
    @Override
	public Status update(String key, ByteIterator value) {
		return insert(key, value);
	}

	/**
	 * Inserts a value for a key. 
	 * @param key
	 * @param value
	 * @return The result of the operation
	 */
    @Override
	public Status insert(String key, ByteIterator value) {
		boolean result = client.put(key, value.toArray());
		if (!result)
			return Status.ERROR;
		return Status.OK;
	}

	/**
	 * Reads the value of for a key. 
	 * @param key The key to read
	 * @param resultValue The found value. Only the first element is used. 
	 * @return The result of the operation
	 */
    @Override
	public Status read(String key, Set<ByteIterator> resultValue) {
		byte[] result = client.get(key);
		if (result == null)
			return Status.NOT_FOUND;
		resultValue.add(new ByteArrayByteIterator(result));
		return Status.OK;
	}
	
	@Override
	public Status swap (String key1, String key2){return Status.OK;}

    @Override
    public Status rotx(Set<String> key1, HashMap<String, ByteIterator> results) {
        return null;
    }

}
