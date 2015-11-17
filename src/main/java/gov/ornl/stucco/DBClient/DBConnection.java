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

import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientDynaElementIterable;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.rexster.client.RexProException;
import com.tinkerpop.rexster.client.RexsterClient;
import com.tinkerpop.rexster.client.RexsterClientFactory;

public class DBConnection {

	private RexsterClient client = null;
	private OrientGraph graph = null;
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
	
	public static OrientGraph getOrientGraph(Configuration configOpts) throws IOException{
	    OrientGraph graph = null;
	    Logger logger = LoggerFactory.getLogger(DBConnection.class);

	    logger.info("connecting to DB...");

	    try {
	        // extract configuration for the DB of interest
	        graph = new OrientGraph(configOpts.getString("graph-name"));
	    } catch (Exception e) {
	        logger.warn(e.getLocalizedMessage());
	        logger.warn(getStackTrace(e));
	        graph.shutdown();
	        throw new IOException("could not create OrientDB client connection");
	    }

	    return graph;
    }
	
	public OrientGraph getGraph() {
	    return graph;
	}

	/*
	 * Note that connectionWaitTime is in seconds
	 */
	public static RexsterClient createClient(Configuration configOpts, int connectionWaitTime) throws IOException{
		RexsterClient client = null;
		Logger logger = LoggerFactory.getLogger(DBConnection.class);

		logger.info("connecting to DB...");

		try {
		    // extract configuration for the DB of interest
		    // client = OrientGraph(/*put path of db here*/)
			client = RexsterClientFactory.open(configOpts); //this just throws "Exception."  bummer.
		} catch (Exception e) {
			logger.warn(e.getLocalizedMessage());
			logger.warn(getStackTrace(e));
			throw new IOException("could not create rexster client connection");
		}

//		//if wait time given, then wait that long, so the connection can set up.  (Mostly needed for travis-ci tests)
//		if(connectionWaitTime > 0){
//			try {
//				logger.info( "waiting for " + connectionWaitTime + " seconds for connection to establish..." );
//				Thread.sleep(connectionWaitTime*1000); //in ms.
//			}
//			catch (InterruptedException ie) { 
//				// Restore the interrupted status
//				Thread.currentThread().interrupt();
//			}
//		}

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

	public static void closeGraph(OrientGraph graph){
		if(graph != null){
		    graph.shutdown();
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
	
	public DBConnection(OrientGraph g){
	    //TODO
	    logger = LoggerFactory.getLogger(DBConnection.class);
	    vertIDCache = new HashMap<String, String>((int) (VERT_ID_CACHE_LIMIT * 1.5));
	    vertIDCacheRecentlyRead = new HashSet<String>((int) (VERT_ID_CACHE_LIMIT * 1.5));
	    graph = g;
	}
	
	public void createIndices() throws IOException{
		// Not sure if this is required other than for tests
	}
	
	public void addVertexFromJSON(JSONObject vert) throws RexProException, IOException{
		addVertexFromMap(jsonVertToMap(vert));
	}

	/** Adds a vertex, given a map of its properties as key-value pairs. */
	public void addVertexFromMap(Map<String, Object> vert) {
		String name = (String)vert.get("name");
		String id = (String)vert.get("_id");
		if(name == null || name.isEmpty()){
			name = id;
			vert.put("name", name);
		}
		
		//convert any multi-valued properties to a list form.
		for(Map.Entry<String, Object> entry : vert.entrySet()) {
		    String key = entry.getKey();
		    Object value = entry.getValue();
		    Object newValue = convertMultiValueToList(value);
		    if(newValue != value) {
		        vert.put(key, newValue);
		    }
		}
		vert.remove("_properties");

		vert.remove("_id"); //Some graph servers will ignore this ID, some won't.  Just remove them so it's consistent.
		OrientVertex v = graph.addVertex("class:V", vert);
		graph.commit();
		return;
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
        String query = String.format("Select * from [%s,%s]", outv_id, inv_id);
        List<OrientVertex> vertexList = getVerticesFromQuery(query);
        if(vertexList != null && vertexList.size() == 2) {
            vertexList.get(0).addEdge(label, vertexList.get(1), null /*iClassName*/, null /*iClusterName*/, props);
            
            // alternate way to create an edge using both vertex's, we need another call to specify the properties
//            String id = String.format("class:%s",label);
//            OrientEdge e =graph.addEdge((Object)id, vertexList.get(0), vertexList.get(1), label);
//            e.setProperties(props);
            graph.commit();
        } else {
            // TODO: if one or more vertices are missing nothing can happen.
            // should log that this happened, especially if we are giving this routine
            // the two vertices it suppose to connect to.
            return;
        }
        
        /* Alternative way to creating an edge using SQL, not sure how to specify  */
        // this approach does not require us to get the vertex references, but it also
        // doesn't allow us to specify the properties...
//        query = String.format("Create edge E FROM %s to %s SET label='%s'", outv_id, inv_id,label);
//        OrientDynaElementIterable qiterable = graph.command(new OCommandSQL(query)).execute();
    }
    
//	private void commit() throws RexProException, IOException{
//	    execute("g.commit()"); 
//	}
	
//	//tries to commit, returns true if success.
//	private boolean tryCommit(){
//		try{
//			commit();
//		}catch(Exception e){
//		    // TODO: should do logging here
//			return false;
//		}
//		return true;
//	}
	
//	//tries to commit, up to 'limit' times. returns true if success.
//	private boolean tryCommit(int limit){
//		int count = 0;
//		boolean result = false;
//		while(!result && count < limit){
//			result = tryCommit();
//			count += 1;
//		}
//		return result;
//	}

//	//TODO make private
//	//wrapper to reduce boilerplate
//	//TODO wrapper throws away any return value, 
//	//  it'd be nice to use this even when we want the query's retval... but then we're back w/ exceptions & don't gain much.
//	public boolean execute(String query, Map<String,Object> params) throws RexProException, IOException{
//		if(this.client == null)
//			return false;
//		//Adding a trailing return 'g' on everything: 
//		// no execute() args can end up returning null, due to known API bug.
//		// returning 'g' everywhere is just the simplest workaround for it, since it is always defined.
//		query += ";g";
//		client.execute(query, params);
//		return true;
//	}
//	//likewise.
//	public boolean execute(String query) throws RexProException, IOException{
//		return execute(query,null);
//	}

	//should only use in tests...
	public RexsterClient getClient(){
		return client;
	}

	/** 
	 * Gets the properties of a vertex selected by RID. 
	 * 
	 * @throws OCommandExecutionException on bad query
	 * 
	 * @return Map of properties (or null if vertex not found)
	 */
	public Map<String, Object> getVertByID(String id) throws OCommandExecutionException {
	    if(id == null || id.isEmpty())
            return null;
	    
		String query = String.format("Select from %s", id);
		List<OrientVertex> vertexList = getVerticesFromQuery(query);
		if(vertexList.isEmpty()){
            return null;
		}
		
		return addPropertiesKey(vertexList).get(0);
	}
	
	/**
	 * Adds the "_properties" key to the map that is copy of the original map.
	 * 
	 * @param vertexList
	 * 
	 * @return  List of a Map of properties (or an empty list if no vertices)
	 */
	private List<Map<String, Object>> addPropertiesKey(List<OrientVertex> vertexList)
	{
	    List<Map<String,Object>> listPropertyMap = new ArrayList<Map<String,Object>>();
	    Map<String, Object> propertyMap = null;
	    for (OrientVertex v : vertexList) {
	        propertyMap = v.getProperties();
	        Map<String,Object> existingProperties = new HashMap<String, Object>(propertyMap);
	        propertyMap.put("_properties", existingProperties);
	        listPropertyMap.add(propertyMap);
	    }
	    return listPropertyMap;
	}

	/** 
     * Gets the properties of a vertex selected by (the property called) name. 
     * 
     * @throws OCommandExecutionException on bad query
     * 
     * @return Map of properties (or null if vertex not found)
     */
	public Map<String,Object> findVert(String name) throws OCommandExecutionException{
		if(name == null || name.isEmpty())
			return null;

		String query = String.format("Select * from V where name='%s'", name);
		List<OrientVertex> vertexList = getVerticesFromQuery(query);

		if(vertexList.size() == 0){
			//logger.info("findVert found 0 matching verts for name:" + name); //this is too noisy, the invoking function can complain if it wants to...
			return null;
		}else if(vertexList.size() > 1){
			logger.warn("findVert found more than 1 matching verts for name: " + name + " so returning the first item.");
			//return null;
		}

		return addPropertiesKey(vertexList).get(0);
	}

	/** 
	 * Runs SQL query to get vertices.
	 * 
	 * @throws OCommandExecutionException on bad query
	 * 
	 * @return Zero or more vertices
	 * */
    public List<OrientVertex> getVerticesFromQuery(String query) throws OCommandExecutionException {
        OrientDynaElementIterable qiterable = graph.command(new OCommandSQL(query)).execute();
        List<OrientVertex> vertexList = new ArrayList<OrientVertex>(1);
        if (qiterable != null) { // Don't know if this can happen, but just in case
            Iterator<Object> iter = qiterable.iterator();
            while (iter.hasNext()) {
                vertexList.add((OrientVertex) iter.next());
            }
        }
        return vertexList;
    }
    
    /** 
     * Runs SQL query to get number of vertices.
     * 
     * @throws OCommandExecutionException on bad query
     * 
     * @return Zero or more vertices
     * */
    private int getVertexCountFromQuery(String query) throws OCommandExecutionException {
        int count = 0;
        OrientDynaElementIterable qiterable = graph.command(new OCommandSQL(query)).execute();
        if (qiterable != null) { // Don't know if this can happen, but just in case
            count = (Integer)qiterable.iterator().next();
        }
        return count;
    }

	//function will check vertIDCache first, if id is not in there, then it is calling the findVert function
	public String findVertId(String name) throws OCommandExecutionException {
		String id = vertIDCacheGet(name);
		if(id != null){
			return id;
		}else{
			Map<String, Object> vert = findVert(name);
			if(vert == null) 
				id = null;
			else 
				id = vert.get("@rid").toString();
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


	public List<Map<String,Object>> findAllVertsWithProps(List<Constraint> constraints) throws OCommandExecutionException {
		if(constraints == null || constraints.size() == 0)
			return null;

		Map<String, Object> param = new HashMap<String, Object>();
		String query = String.format("SELECT FROM V where ");
		for(int i=0; i<constraints.size(); i++){
			Constraint c = constraints.get(i);
			String cond = c.condString(c.cond);
			String key = c.prop.toUpperCase()+i;
			param.put(key, c.val);
			if(i > 0 ) {
			    query += " AND ";
			}
			query += String.format(" %s %s '%s' ", c.prop, cond, c.val);
		}
		
		List<OrientVertex> verts = this.getVerticesFromQuery(query);
		List<Map<String,Object>> verticeProperties = addPropertiesKey(verts);
		return verticeProperties;
	}

	/*
	 * @deprecated use getEdgeCount instead
	 */
	@Deprecated
	public boolean edgeExists(String inv_id, String outv_id, String label) throws OCommandExecutionException {
		return (getEdgeCount(inv_id, outv_id, label) > 0);
	}
	
	 /*
     * returns edge count, or -1 if IDs not found. Throws exceptions if other error occurred.
     */
    public int getEdgeCount(String inv_id, String outv_id, String label) throws OCommandExecutionException {
        int retValue = getEdgeCountOrientDB(inv_id, outv_id, label);
        return retValue;
    }

	/*
     * returns edge count, or -1 if IDs not found. Throws exceptions if other error occurred.
     */
    private int getEdgeCountOrientDB(String inv_id, String outv_id, String label) throws OCommandExecutionException {
        int edgeCount = 0;
        if(inv_id == null || inv_id.isEmpty() || outv_id == null || outv_id.isEmpty() || label == null || label.isEmpty())
            return -1;

        Map<String, Object> param = new HashMap<String, Object>();
        param.put("ID_OUT", outv_id);
        param.put("ID_IN", inv_id);
        param.put("LABEL", label);
        Object query_ret;

        query_ret = getVertByID(outv_id);
        if(query_ret == null){
            logger.warn("getEdgeCount could not find out_id:" + outv_id);
            return -1;
        }
        query_ret = getVertByID(inv_id);
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
            String query = String.format("SELECT expand(out('%s')) FROM %s",label, outv_id); 
            List<OrientVertex>results = getVerticesFromQuery(query);
            
            for(OrientVertex item : results){
                String idvalue = item.getProperties().get("@rid").toString();
                if(inv_id.equals(idvalue))
                    edgeCount++;
            }
            return edgeCount;
        }else{
            String query = String.format("SELECT expand(in('%s')) FROM %s",label, inv_id); 
            List<OrientVertex>results = getVerticesFromQuery(query);
            
            for(OrientVertex item : results){
                String idvalue = item.getProperties().get("@rid").toString();
                if(outv_id.equals(idvalue))
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
        graph.commit();
    }
    
	@SuppressWarnings({ "rawtypes", "unchecked" })
    public void updateVertPropertyOrientDB(String id, String key, Object val) throws OCommandExecutionException{ 
		String cardinality = findCardinality(id, key, val);
		if(cardinality == null){
			cardinality = "SINGLE";
			cardinalityCache.put(key, cardinality);
		}
		
		if (key.equals("_properties")) {
		    return;
		}

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

		String query = String.format("SELECT FROM %s",id);
		List<OrientVertex> verts = this.getVerticesFromQuery(query);
		if(!verts.isEmpty()) {
//		    execute("g.v(ID).setProperty(KEY, VAL);g.commit()", param);
		    verts.get(0).setProperty(key, val);
		}
//		tryCommit(COMMIT_TRY_LIMIT);
	}
	
	/**
	 * returns cardinality of property "key" from vertex id.  If not found, returns null.
	 */
	private String findCardinality(String id, String key, Object val) throws OCommandExecutionException {
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
		graph.command(new OCommandSQL("DELETE VERTEX V")).execute();
//		graph.commit();
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
