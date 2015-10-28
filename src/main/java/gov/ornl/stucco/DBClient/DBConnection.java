package gov.ornl.stucco.DBClient;

import gov.ornl.stucco.DBClient.Constraint.Condition;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.tinkerpop.rexster.client.RexProException;
import com.tinkerpop.rexster.client.RexsterClient;
import com.tinkerpop.rexster.client.RexsterClientFactory;

public class DBConnection {

	private RexsterClient client = null;
	private Logger logger = null;
	private Map<String, String> vertIDCache = null; //TODO could really split this into a simple cache class.
	private Set<String> vertIDCacheRecentlyRead = null;
	private static int VERT_ID_CACHE_LIMIT = 10000;
	private Map<String,String> cardinalityCache = new HashMap<String, String>(200);
	private static int WRITE_CONFIRM_TRY_LIMIT = 6;
	private static int COMMIT_TRY_LIMIT = 4;
	private static String[] HIGH_FORWARD_DEGREE_EDGE_LABELS = {"hasFlow"}; //TODO: update as needed.  Knowing these allows some queries to be optimized.
	private static String[] HIGH_REVERSE_DEGREE_EDGE_LABELS = {"hasIP", "hasPort", "hasVulnerability", "inAddressRange"}; //TODO: like above, but currently unused (because most queries don't care about the reverse degree).

	public static RexsterClient createClient(Configuration configOpts) throws IOException{
		return createClient(configOpts, 0);
	}

	/*
	 * Note that connectionWaitTime is in seconds
	 */
	public static RexsterClient createClient(Configuration configOpts, int connectionWaitTime) throws IOException{
		RexsterClient client = null;
		Logger logger = LoggerFactory.getLogger(DBConnection.class);

		logger.info("connecting to DB...");

		try {
			client = RexsterClientFactory.open(configOpts); //this just throws "Exception."  bummer.
		} catch (Exception e) {
			logger.warn(e.getLocalizedMessage());
			logger.warn(getStackTrace(e));
			throw new IOException("could not create rexster client connection");
		}

		//if wait time given, then wait that long, so the connection can set up.  (Mostly needed for travis-ci tests)
		if(connectionWaitTime > 0){
			try {
				logger.info( "waiting for " + connectionWaitTime + " seconds for connection to establish..." );
				Thread.sleep(connectionWaitTime*1000); //in ms.
			}
			catch (InterruptedException ie) { 
				// Restore the interrupted status
				Thread.currentThread().interrupt();
			}
		}

		return client;
	}

	public static Configuration getDefaultConfig(){
		Logger logger = LoggerFactory.getLogger(DBConnection.class);
		logger.info("Loading default DB Config...");
		Configuration configOpts = dbConfigFromFile("config.yaml");
		return configOpts;
	}

	public static Configuration getTestConfig(){
		Logger logger = LoggerFactory.getLogger(DBConnection.class);
		logger.info("Loading test DB Config...");
		Configuration configOpts = dbConfigFromFile("rexster-test-config.yaml");
		return configOpts;
	}

	public static void closeClient(RexsterClient client){
		if(client != null){
			try {
				client.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			client = null;
		}
	}

	public DBConnection() throws IOException{
		this(createClient(getDefaultConfig()));
	}

	public DBConnection(RexsterClient c){
		//TODO
		logger = LoggerFactory.getLogger(DBConnection.class);
		vertIDCache = new HashMap<String, String>((int) (VERT_ID_CACHE_LIMIT * 1.5));
		vertIDCacheRecentlyRead = new HashSet<String>((int) (VERT_ID_CACHE_LIMIT * 1.5));
		client = c;
	}
	
	public void createIndices() throws IOException{
		// Not sure if this is required other than for tests
	}
	
	public void addVertexFromJSON(JSONObject vert) throws RexProException, IOException{
		addVertexFromMap(jsonVertToMap(vert));
	}

	/** Adds a vertex, given a map of its properties as key-value pairs. */
	public void addVertexFromMap(Map<String, Object> vert) throws RexProException, IOException{
		String newID = null;
		String name = (String)vert.get("name");
		String id = (String)vert.get("_id");
		if(name == null || name.isEmpty()){
			name = id;
			vert.put("name", name);
		}
		
		//convert any multivalued properties to a list form.
		Set<String> keySet = new HashSet<String>( (Set<String>)vert.keySet() );
		for(Map.Entry<String, Object> entry : vert.entrySet()) {
		    String key = entry.getKey();
		    Object value = entry.getValue();
		    Object newValue = convertMultiValueToList(value);
		    if(newValue != value) {
		        vert.put(key, newValue);
		    }
		}

		vert.remove("_id"); //Some graph servers will ignore this ID, some won't.  Just remove them so it's consistent.
		Map<String, Object> param = new HashMap<String, Object>();
		param.put("VERT_PROPS", vert);

		newID = (String)client.execute("v = g.addVertex(null, VERT_PROPS);g.commit();v.getId();", param).get(0);		
	}

	/**
	 * Converts multi-valued Object to List, but leaves other Objects alone.
	 */
    private Object convertMultiValueToList(Object value) {
        
        List newValue = new ArrayList();
        if (value instanceof Set) {
            newValue = new ArrayList((Set) value);
        }
        else if (value instanceof JSONArray ) {
            for(int i=0; i<((JSONArray)value).length(); i++){
                Object currVal = ((JSONArray)value).get(i);
                newValue.add(currVal);
            }
        }
        else if(value instanceof Object[]) {
            for(int i=0; i<((Object[])value).length; i++){ 
                Object currVal = ((Object[])value)[i];
                newValue.add(currVal);
            }
        } else {
            return value;
        }
        
        return newValue;
    }

    public void addEdgeFromJSON(JSONObject edge) throws RexProException, IOException{
        Map<String, Object> param = new HashMap<String, Object>();

        String outv_id = findVertId(edge.getString("_outV"));
        String inv_id = findVertId(edge.getString("_inV"));
        String edgeName = edge.getString("_id");
        if(outv_id == null){
            return;
        }
        if(inv_id == null){
            return;
        }
        String label = edge.optString("_label");
        if(getEdgeCount(inv_id, outv_id, label) >= 1){
            //edge already exists, do nothing and return false.
            // (if you wanted to update its properties, this is not the method for that)
            return;
        }
        param.put("ID_OUT", outv_id);
        param.put("ID_IN", inv_id);
        param.put("LABEL", label);
        //build your param map obj
        Map<String, Object> props = new HashMap<String, Object>();
        props.put("edgeName", edgeName);
        edge.remove("_inv");
        edge.remove("_outv");
        edge.remove("_id");
        Iterator<String> k = edge.keys();
        String key;
        while(k.hasNext()){
            key = k.next();
            props.put(key, edge.get(key));
        }
        
        //and now finally add edge to graph.  If it fails, return false here. if it was ok, then we can continue below.
        param.put("EDGE_PROPS", props);
        execute("g.addEdge(g.v(ID_OUT),g.v(ID_IN),LABEL,EDGE_PROPS); g.commit()", param);
    }
    
	private void commit() throws RexProException, IOException{
	    execute("g.commit()"); 
	}
	
	//tries to commit, returns true if success.
	private boolean tryCommit(){
		try{
			commit();
		}catch(Exception e){
		    // TODO: should do logging here
			return false;
		}
		return true;
	}
	
	//tries to commit, up to 'limit' times. returns true if success.
	private boolean tryCommit(int limit){
		int count = 0;
		boolean result = false;
		while(!result && count < limit){
			result = tryCommit();
			count += 1;
		}
		return result;
	}

	//TODO make private
	//wrapper to reduce boilerplate
	//TODO wrapper throws away any return value, 
	//  it'd be nice to use this even when we want the query's retval... but then we're back w/ exceptions & don't gain much.
	public boolean execute(String query, Map<String,Object> params) throws RexProException, IOException{
		if(this.client == null)
			return false;
		//Adding a trailing return 'g' on everything: 
		// no execute() args can end up returning null, due to known API bug.
		// returning 'g' everywhere is just the simplest workaround for it, since it is always defined.
		query += ";g";
		client.execute(query, params);
		return true;
	}
	//likewise.
	public boolean execute(String query) throws RexProException, IOException{
		return execute(query,null);
	}

	//should only use in tests...
	public RexsterClient getClient(){
		return client;
	}

	public Map<String, Object> getVertByID(String id) throws RexProException, IOException{
		Map<String, Object> param = new HashMap<String, Object>();
		param.put("ID", id);
		
		Object query_ret = client.execute("g.v(ID).map();", param);
		List<Map<String, Object>> query_ret_list = (List<Map<String, Object>>)query_ret;
		Map<String, Object> query_ret_map = query_ret_list.get(0);
		return query_ret_map;
	}

	public Map<String,Object> findVert(String name) throws IOException, RexProException{
		if(name == null || name.isEmpty())
			return null;
		Map<String, Object> param = new HashMap<String, Object>();
		param.put("NAME", name);
		Object query_ret = client.execute("g.query().has(\"name\",NAME).vertices().toList();", param);
		List<Map<String,Object>> query_ret_list = (List<Map<String,Object>>)query_ret;
		//logger.info("query returned: " + query_ret_list);
		if(query_ret_list.size() == 0){
			//logger.info("findVert found 0 matching verts for name:" + name); //this is too noisy, the invoking function can complain if it wants to...
			return null;
		}else if(query_ret_list.size() > 1){
			logger.warn("findVert found more than 1 matching verts for name: " + name + " so returning the first item.");
			//return null;
		}

		return query_ret_list.get(0);
	}

	//function will check vertIDCache first, if id is not in there, then it is calling the findVert funciton
	public String findVertId(String name) throws IOException, RexProException{
		String id = vertIDCacheGet(name);
		if(id != null){
			return id;
		}else{
			Map<String, Object> vert = findVert(name);
			if(vert == null) 
				id = null;
			else 
				id = (String)vert.get("_id");
			if(id != null){
				vertIDCachePut(name, id);
			}
			return id;
		}
	}

	public List<Map<String,Object>> findAllVertsByType(String vertexType) throws IOException, RexProException{
		if(vertexType == null || vertexType.isEmpty())
			return null;

		Map<String, Object> properties = new HashMap<String, Object>();
		List<Constraint> l = new ArrayList<Constraint>();
		Constraint c = new Constraint("vertexType", Condition.eq, vertexType);
		l.add(c);
		List<Map<String,Object>> query_ret_list = findAllVertsWithProps(l);

		if(query_ret_list.size() == 0){
			logger.warn("findAllVertsByType found 0 matching verts for type:" + vertexType);
			return null;
		}
		return query_ret_list;
	}


	public List<Map<String,Object>> findAllVertsWithProps(List<Constraint> constraints) throws IOException, RexProException{
		if(constraints == null || constraints.size() == 0)
			return null;

		Map<String, Object> param = new HashMap<String, Object>();
		String query = "g.V";
		for(int i=0; i<constraints.size(); i++){
			Constraint c = constraints.get(i);
			String cond = c.condString(c.cond);
			String key = c.prop.toUpperCase()+i;
			param.put(key, c.val);
			query += ".has(\"" + c.prop + "\"," + cond + "," + key + ")";
		}
		query += ";";
		Object query_ret = client.execute(query, param);
		List<Map<String,Object>> query_ret_list = (List<Map<String,Object>>)query_ret;

		return query_ret_list;
	}

	/*
	 * @deprecated use getEdgeCount instead
	 */
	@Deprecated
	public boolean edgeExists(String inv_id, String outv_id, String label) throws RexProException, IOException {
		return (getEdgeCount(inv_id, outv_id, label) > 0);
	}
	
	 /*
     * returns edge count, or -1 if IDs not found. Throws exceptions if other error occurred.
     */
    public int getEdgeCount(String inv_id, String outv_id, String label) throws RexProException, IOException {
        int retValue = getEdgeCountOrientDB(inv_id, outv_id, label);
        return retValue;
    }

	/*
     * returns edge count, or -1 if IDs not found. Throws exceptions if other error occurred.
     */
    private int getEdgeCountOrientDB(String inv_id, String outv_id, String label) throws RexProException, IOException {
        int edgeCount = 0;
        if(inv_id == null || inv_id.isEmpty() || outv_id == null || outv_id.isEmpty() || label == null || label.isEmpty())
            return -1;

        Map<String, Object> param = new HashMap<String, Object>();
        param.put("ID_OUT", outv_id);
        param.put("ID_IN", inv_id);
        param.put("LABEL", label);
        Object query_ret;

        query_ret = client.execute("g.v(ID_OUT);", param);
        if(query_ret == null){
            logger.warn("getEdgeCount could not find out_id:" + outv_id);
            return -1;
        }
        query_ret = client.execute("g.v(ID_IN);", param);
        if(query_ret == null){
            logger.warn("getEdgeCount could not find inv_id:" + inv_id);
            return -1;
        }

        boolean highDegree = false;
        for(String currLabel : HIGH_FORWARD_DEGREE_EDGE_LABELS){
            if(label.equals(currLabel)){
                highDegree = true;
                break;
            }
        }

        if(!highDegree){
            query_ret = client.execute("g.v(ID_OUT).outE(LABEL).inV().id;", param);
            List<String> query_ret_list = (List<String>)query_ret;
            //System.out.println("query ret list contains " + query_ret_list.size() + " items.");
            for(String item : query_ret_list){
                if(inv_id.equals(item))
                    edgeCount++;
            }
            return edgeCount;
        }else{
            query_ret = client.execute("g.v(ID_IN).inE(LABEL).outV().id;", param);
            List<String> query_ret_list = (List<String>)query_ret;
            //System.out.println("query ret list contains " + query_ret_list.size() + " items.");
            for(String item : query_ret_list){
                if(outv_id.equals(item))
                    edgeCount++;
            }
            return edgeCount;
        }
    }

	public void updateVert(String id, Map<String, Object> props) throws RexProException, IOException{
		String[] keys = props.keySet().toArray(new String[0]);
		for(int i=0; i<keys.length; i++){
			updateVertProperty(id, keys[i], props.get(keys[i]));
		}
	}
    public void updateVertProperty(String id, String key, Object val) throws RexProException, IOException{
        updateVertPropertyOrientDB(id, key, val);
    }
    
	@SuppressWarnings({ "rawtypes", "unchecked" })
    public void updateVertPropertyOrientDB(String id, String key, Object val) throws RexProException, IOException{
		HashMap<String, Object> param = new HashMap<String, Object>();

		String cardinality = findCardinality(id, key, val);
		if(cardinality == null){
			cardinality = "SINGLE";
			cardinalityCache.put(key, cardinality);
		}

		param.put("ID", id);
		param.put("KEY", key);

		if (cardinality.equals("SET")) {
		    // At this point, we assume it's in List form
		    
			Map<String, Object> queryRetValue = getVertByID(id);
			if (queryRetValue == null) {
			    // Handle how?
			}
			
			Object obj = convertMultiValueToList(val);
			List newValue;
			if (obj instanceof List) {
			    newValue = (List) obj;
			}
			else {
			    newValue = Collections.singletonList(obj);
			}
			
			List currentList = (List)queryRetValue.get(key);

			if(currentList == null) {
			    // no value existed in the DB
			    val = newValue;
			} else {
			    Set currentSet = new HashSet(currentList);
			    for (Object currVal : newValue) {
			        if (!currentSet.contains(currVal)) {
			            currentSet.add(currVal);
			            currentList.add(currVal);
			        }
			    }
			    val = currentList;
			}
		}
		param.put("VAL", val);
		execute("g.v(ID).setProperty(KEY, VAL);g.commit()", param);
//		tryCommit(COMMIT_TRY_LIMIT);
	}
	
	/**
	 * returns cardinality of property "key" from vertex id.  If not found, returns null.
	 */
	private String findCardinality(String id, String key, Object val) throws RexProException, IOException {
		String cardinality;

		cardinality = cardinalityCache.get(key);
		if(cardinality == null){
		    if(isMultipleCardinality(val)){
                cardinality = "SET";
                cardinalityCache.put(key, cardinality);
            } else {
                // go to DB to see if it has this property, from this vertex id
                Map<String, Object> queryRetMap = getVertByID(id);  
                if (queryRetMap != null) {
                    Object dbVal = queryRetMap.get(key);
                    if (dbVal != null) {
                        if(isMultipleCardinality(dbVal)){
                            cardinality = "SET";
                        }
                        else {
                            cardinality = "SINGLE";
                        }
                        cardinalityCache.put(key, cardinality);
                    }
                } 
            }
		}
		return cardinality;
	}
	
	/** Gets whether the value's data type supports a cardinality of "SET". */
	private boolean isMultipleCardinality(Object value) {
        return (value != null && (value instanceof JSONArray || value instanceof Set || value instanceof List || value instanceof Object[]));
	}

	private void vertIDCachePut(String name, String id){
		if(vertIDCache.size() >= VERT_ID_CACHE_LIMIT){
			logger.info("vertex id cache exceeded limit of " + VERT_ID_CACHE_LIMIT + 
					" ... evicting " + (vertIDCache.size() - vertIDCacheRecentlyRead.size()) + " unused items.");
//			Map<String, String> newVertIDCache = new HashMap<String, String>((int) (VERT_ID_CACHE_LIMIT * 1.5));
//			for(String n : vertIDCacheRecentlyRead){
//				if(vertIDCache.containsKey(n)){
//					newVertIDCache.put(n, vertIDCache.get(n));
//				}
//			}
//			vertIDCacheRecentlyRead = new HashSet<String>((int) (VERT_ID_CACHE_LIMIT * 1.5));
//			vertIDCache = newVertIDCache;
			vertIDCache.keySet().retainAll(vertIDCacheRecentlyRead);
			vertIDCacheRecentlyRead.clear();
		}
		vertIDCache.put(name, id);
	}
	
	private String vertIDCacheGet(String name){
		if(vertIDCache.containsKey(name)){ 
			vertIDCacheRecentlyRead.add(name);
			return vertIDCache.get(name);
		}else{
			return null;
		}
		
	}
	
	
	
	/*
	 * Only used by removeAllVertices()
	 */
	private void removeCachedVertices(){
		//NB: this query is slow enough that connection can time out if the DB starts with many vertices.

		if(vertIDCache.isEmpty())
			return;


		//clear the cache now.
		vertIDCache.clear();
//		= new HashMap<String, String>((int) (VERT_ID_CACHE_LIMIT * 1.5));
		vertIDCacheRecentlyRead.clear();// = new HashSet<String>((int) (VERT_ID_CACHE_LIMIT * 1.5));

//		return ret;

	}

	/*
	 * Only use in tests.
	 */
	public void removeAllVertices(){
		//NB: this query is slow enough that connection can time out if the DB starts with many vertices.
//		boolean ret = false; 
		removeCachedVertices();
		try{
			client.execute("g.V.remove();g.commit()");
		}catch(Exception e){
			e.printStackTrace();
//			ret = false;
		}
//		try{
//			int tryCount = 0;
//			List<Object> queryRet;
//			//Confirm before proceeding
//			while(ret == false && tryCount < WRITE_CONFIRM_TRY_LIMIT){
//				//System.out.println("waiting for " + tryCount + " seconds in removeAllVertices()");
//				waitFor(1000*tryCount +1);
//				commit();
//				queryRet = client.execute("g.V.count();");
//				if( (Long)queryRet.get(0) == 0){
//					ret = true;
//				}
//				tryCount += 1;
//			}
//		}catch(Exception e){
//			logger.warn(e.getLocalizedMessage());
//			logger.warn(getStackTrace(e));
//		}
	}
	
	private void waitFor(int ms){
		try {
			Thread.sleep(ms);
		}
		catch (InterruptedException ie) { 
			// Restore the interrupted status
			Thread.currentThread().interrupt();
		}
	}
	
	private static String getStackTrace(Exception e){
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		return sw.toString();
	}
	
	//see Align class
	public List<Object> jsonArrayToList(JSONArray a){
		List<Object> l = new ArrayList<Object>();
		for(int i=0; i<a.length(); i++){
			l.add(a.get(i));
		}
		return l;
	}
	
	//see Align class	
	public Map<String, Object> jsonVertToMap(JSONObject v){
		Map<String, Object> vert = new HashMap<String, Object>();
		for(Object k : v.keySet()){
			String key = (String) k;
			Object value = v.get(key);
			if(value instanceof JSONArray){
				value = jsonArrayToList((JSONArray)value);
			}
			else if(value instanceof JSONObject){
				logger.warn("jsonVertToMap: unexpected property type: JSONObject for property " + key + "\n" + v);
			}
			vert.put(key, value);
		}
		return vert;
	}

	public static Configuration dbConfigFromFile(String configFilePath){
		File configFile = new File(configFilePath);
		Map<String, Object> fullConfigMap = configMapFromFile(configFile);
		Map<String, Object> dbConfigMap = (Map<String, Object>) fullConfigMap.get("database_connection");
		PropertiesConfiguration dbConfig = new PropertiesConfiguration();
		for(String key : dbConfigMap.keySet()){
			dbConfig.addProperty( key, dbConfigMap.get(key));
		}
		return dbConfig;
	}

	public static Map<String, Object> configMapFromFile(File configFile){
		Map<String, Object> configMap = null;
		try {
			Yaml yaml = new Yaml();
			InputStream stream = new FileInputStream(configFile);

			configMap = (Map<String, Object>) yaml.load( stream );

			//config.load(configFile);

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return configMap;
	}

}
