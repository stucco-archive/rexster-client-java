package gov.ornl.stucco.DBClient;

import gov.ornl.stucco.DBClient.Constraint;
import gov.ornl.stucco.DBClient.Constraint.Condition;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.graphdb.types.vertices.PropertyKeyVertex;
import com.tinkerpop.rexster.client.RexProException;
import com.tinkerpop.rexster.client.RexsterClient;
import com.tinkerpop.rexster.client.RexsterClientFactory;
import com.tinkerpop.rexster.client.RexsterClientTokens;
import com.tinkerpop.rexster.protocol.serializer.msgpack.MsgPackSerializer;
import com.tinkerpop.blueprints.*;

public class DBConnection {

	private RexsterClient client = null;
	private Logger logger = null;
	private Map<String, String> vertIDCache = null;
	private Map<String, String> cardinalityCache = null;
	private String dbType = null;
	private static int WRITE_CONFIRM_TRY_LIMIT = 10;
	private static int COMMIT_TRY_LIMIT = 4;

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
		vertIDCache = new HashMap<String, String>(10000);
		cardinalityCache = new HashMap<String, String>(200);
		client = c;
	}

	private String getDBType() throws IOException{
		if(this.dbType == null){
			String type = null;
			try{
				type = client.execute("g.getClass()").get(0).toString();
			}catch(Exception e){
				logger.error("Could not find graph type!",e);
				throw new IOException("Could not find graph type!");
			}
			if( type.equals("class com.tinkerpop.blueprints.impls.tg.TinkerGraph") ){
				this.dbType = "TinkerGraph";
			}else if( type.equals("class com.thinkaurelius.titan.graphdb.database.StandardTitanGraph") ){
				this.dbType = "TitanGraph";
			}else{
				throw new IOException("Could not find graph type - unknown type!");
			}
		}
		return this.dbType;
	}


	public void createIndices() throws IOException{
		String graphType = getDBType();
		if( graphType.equals("TinkerGraph") ){
			createTinkerGraphIndices();
		}else if( graphType.equals("TitanGraph") ){
			createTitanIndices();
		}else{
			logger.warn("unknown graph type!  Assuming it is Titan...");
			createTitanIndices();
		}
	}


	private void createTinkerGraphIndices(){
		List<String> currentIndices = new ArrayList<String>();
		try {
			currentIndices = client.execute("g.getIndexedKeys(Vertex.class)");
		} catch (Exception e) { 
			//this.client = null;
			logger.error("problem getting indexed keys, assuming there were none...");
			logger.error("Exception was: ",e);
		}
		logger.info( "found vertex indices: " + currentIndices );
		try {
			if(!currentIndices.contains("name")){
				logger.info("'name' key index not found, creating ...");
				client.execute("g.createKeyIndex('name', Vertex.class);g");
			}
			if(!currentIndices.contains("vertexType")){
				logger.info("'vertexType' key index not found, creating ...");
				client.execute("g.createKeyIndex('vertexType', Vertex.class);g");
			}
			if(!currentIndices.contains("ipInt")){
				logger.info("'ipInt' key index not found, creating ...");
				client.execute("g.createKeyIndex('ipInt', Vertex.class);g");
			}
			if(!currentIndices.contains("startIPInt")){
				logger.info("'startIPInt' key index not found, creating ...");
				client.execute("g.createKeyIndex('startIPInt', Vertex.class);g");
			}
			if(!currentIndices.contains("endIPInt")){
				logger.info("'endIPInt' key index not found, creating ...");
				client.execute("g.createKeyIndex('endIPInt', Vertex.class);g");
			}
		} catch (RexProException e) {
			logger.error("Exception was: ",e.getLocalizedMessage());
			logger.warn(getStackTrace(e));
		} catch (IOException e) {
			logger.error("Exception was: ",e.getLocalizedMessage());
			logger.warn(getStackTrace(e));
		}
	}

	private void createTitanIndices(){
		List currentIndices = null;
		try {
			//configure vert indices needed
			//List currentIndices = client.execute("g.getManagementSystem().getGraphIndexes(Vertex.class)");
			currentIndices = client.execute("g.getIndexedKeys(Vertex.class)");
		} catch (Exception e) { 
			//this.client = null;
			logger.error("problem getting indexed keys, assuming there were none...");
			logger.error("Exception was: ",e);
		}
		logger.info( "found vertex indices: " + currentIndices );
		try{
			//		System.out.println("currentIndices = " + currentIndices +  " " + "name");
			if(currentIndices == null || !currentIndices.contains("name")){
				List names = client.execute("mgmt = g.getManagementSystem();mgmt.getPropertyKey(\"name\");");
				//logger.info("name found: ", names.get(0));
				if(names.get(0) == null){
					logger.info("'name' variable and index not found, creating var and index...");
					client.execute("mgmt = g.getManagementSystem();"
							+ "name = mgmt.makePropertyKey(\"name\").dataType(String.class).make();"
							+ "mgmt.buildIndex(\"byName\",Vertex.class).addKey(name).unique().buildCompositeIndex();"
							+ "mgmt.commit();g;");
				}else{
					logger.info("'name' was found, but not indexed.  creating index...");
					client.execute("mgmt = g.getManagementSystem();"
							+ "name = mgmt.getPropertyKey(\"name\");"
							+ "mgmt.buildIndex(\"byName\",Vertex.class).addKey(name).unique().buildCompositeIndex();"
							+ "mgmt.commit();g;");
				}
			}
			if(currentIndices == null || !currentIndices.contains("vertexType")){
				List names = client.execute("mgmt = g.getManagementSystem();mgmt.getPropertyKey(\"vertexType\");");
				//logger.info("vertexType found: ", names.get(0));
				if(names.get(0) == null){
					logger.info("'vertexType' variable and index not found, creating var and index...");
					client.execute("mgmt = g.getManagementSystem();"
							+ "vertexType = mgmt.makePropertyKey(\"vertexType\").dataType(String.class).make();"
							+ "mgmt.buildIndex(\"byVertexType\",Vertex.class).addKey(vertexType).buildCompositeIndex();"
							+ "mgmt.commit();g;");
				}else{
					logger.info("'vertexType' was found, but not indexed.  creating index...");
					client.execute("mgmt = g.getManagementSystem();"
							+ "vertexType = mgmt.getPropertyKey(\"vertexType\");"
							+ "mgmt.buildIndex(\"byVertexType\",Vertex.class).addKey(vertexType).buildCompositeIndex();"
							+ "mgmt.commit();g;");
				}
			}
			/*
			if(!currentIndices.contains("name") || !currentIndices.contains("vertexType")){
				logger.info("name or vertexType index not found, creating combined index...");
				client.execute("mgmt = g.getManagementSystem();"
						+ "name = mgmt.getPropertyKey(\"name\");"
						+ "vertexType = mgmt.getPropertyKey(\"vertexType\");"
						+ "mgmt.buildIndex(\"byNameAndVertexType\",Vertex.class).addKey(name).addKey(vertexType).unique().buildCompositeIndex();"
						+ "mgmt.commit();g;"); //TODO: not convinced that this (new) index really works, need to test further.  but it's currently unused, so leaving as-is for now.
			}*/
			commit();
			logger.info("Connection is good!");
		}catch(Exception e){
			logger.warn("could not configure missing vertex indices!", e.getLocalizedMessage());
			logger.warn(getStackTrace(e));
			//NB: this is (typically) non-fatal.  Multiple workers can attempt to create the indices at the same time, and some will just fail in this way.
			//TODO: these need to either be created in one thread only, or else use proper locking.
			//this.client = null;
			//logger.error("Connection is unusable!");
		}
		/*
		currentIndices = client.execute("g.getIndexedKeys(Edge.class)");
		logger.info( "found edge indices: " + currentIndices );
		try{
			if(!currentIndices.contains("name")){
				logger.info("name index not found, creating...");
				client.execute("g.makeKey(\"edgeName\").dataType(String.class).indexed(\"standard\",Edge.class).unique().make();g.commit();g;");
			}
		}catch(Exception e){
			logger.error("could not configure missing indices!", e);
		}
		 */
	}

	public boolean addVertexFromJSON(JSONObject vert) throws RexProException, IOException{
		return addVertexFromMap(jsonVertToMap(vert));
	}

	public boolean addVertexFromMap(Map<String, Object> vert) throws RexProException, IOException{
		boolean ret = false;
		Long newID = null;
		String graphType = getDBType();
		String name = (String)vert.get("name");
		//System.out.println("vertex name is: " + name);
		String id = (String)vert.get("_id");
		//System.out.println("vertex id is: " + id);
		if(name == null || name == ""){
			name = id;
			vert.put("name", name);
		}
		//any properties that aren't cardinality "SINGLE" can't be handled this way, handle them later. 
		Map<String, Object> specialCardProps = new HashMap<String, Object>();
		Set<String> keySet = new HashSet<String>( (Set<String>)vert.keySet() );
		for(String key : keySet){
			if(findCardinality(key) != null && !findCardinality(key).equalsIgnoreCase("SINGLE")){
				specialCardProps.put(key, vert.get(key));
				vert.remove(key);
			}
		}
		vert.remove("_id"); //Some graph servers will ignore this ID, some won't.  Just remove them so it's consistent.
		Map<String, Object> param = new HashMap<String, Object>();
		param.put("VERT_PROPS", vert);
		
		if(graphType == "TitanGraph")
			newID = (Long)client.execute("v = g.addVertex(null, VERT_PROPS);v.getId();", param).get(0);
		//newID = (Long)client.execute("v = GraphSONUtility.vertexFromJson(VERT_PROPS, new GraphElementFactory(g), GraphSONMode.NORMAL, null);v.getId()", param).get(0);
		if(graphType == "TinkerGraph")
			newID = Long.parseLong((String)client.execute("v = g.addVertex(null, VERT_PROPS);v.getId();", param).get(0));
		//newID = Long.parseLong((String)client.execute("v = GraphSONUtility.vertexFromJson(VERT_PROPS, new GraphElementFactory(g), GraphSONMode.NORMAL, null);v.getId()", param).get(0));
		//System.out.println("new ID is: " + newID);
		vertIDCache.put(name, newID.toString());
		//handling the non-"SINGLE" cardinality properties now.
		for(String key : specialCardProps.keySet()){
			updateVertProperty(newID.toString(), key, specialCardProps.get(key));
		}
		
		//confirm before proceeding
		ret = false;
		tryCommit(COMMIT_TRY_LIMIT);
		int tryCount = 0;
		//Confirm before proceeding
		while(ret == false && tryCount < WRITE_CONFIRM_TRY_LIMIT){
			//System.out.println("waiting for " + tryCount + " seconds in addVertexFromMap()");
			waitFor(1000*tryCount + 1);
			if( getVertByID(newID.toString()) != null && findVert(name) != null){
				ret = true;
			}
			tryCount += 1;
		}

		return ret;
	}

	public boolean addEdgeFromJSON(JSONObject edge) throws RexProException, IOException{
		boolean ret = false;
		Map<String, Object> param = new HashMap<String, Object>();

		//System.out.println("edge outV is " + edge.getString("_outV"));
		String outv_id = findVertId(edge.getString("_outV"));
		String inv_id = findVertId(edge.getString("_inV"));
		String edgeName = edge.getString("_id");
		//System.out.println("ID = " + edgeName);
		//String edgeID = findEdgeId(edgeName);
		if(outv_id == null){
			//logger.error("Could not find out_v for edge: " + edge);
			return false;
		}
		if(inv_id == null){
			//logger.error("Could not find in_v for edge: " + edge);
			return false;
		}
		String label = edge.optString("_label");
		if(getEdgeCount(inv_id, outv_id, label) >= 1){
			//edge already exists, do nothing and return false.
			// (if you wanted to update its properties, this is not the method for that)
			//logger.debug("Attempted to add a duplicate edge.  ignoring .  Edge was " + edge);
			return false;
		}
		param.put("ID_OUT", Integer.parseInt(outv_id));
		param.put("ID_IN", Integer.parseInt(inv_id));
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
			//	System.out.println(key);
		}
		
		//and now finally add edge to graph.  If it fails, return false here. if it was ok, then we can continue below.
		param.put("EDGE_PROPS", props);
		ret = execute("g.addEdge(g.v(ID_OUT),g.v(ID_IN),LABEL,EDGE_PROPS)", param);
		if(ret == false){ 
			return ret;
		}
		
		//confirm before proceeding
		
		ret = false;
		tryCommit(COMMIT_TRY_LIMIT);
		int tryCount = 0;
		//Confirm before proceeding
		while(ret == false && tryCount < WRITE_CONFIRM_TRY_LIMIT){
			//System.out.println("waiting for " + tryCount + " seconds in addEdgeFromJSON()");
			waitFor(1000*tryCount + 1);
			if( getEdgeCount(inv_id, outv_id, label) >= 1){
				ret = true;
			}
			tryCount += 1;
		}
		
		return ret;
	}

	private void commit() throws RexProException, IOException{
		String graphType = getDBType();
		if(graphType != "TinkerGraph")
			execute("g.commit()");
	}
	
	//tries to commit, returns true if success.
	private boolean tryCommit(){
		try{
			commit();
		}catch(Exception e){
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
		param.put("ID", Integer.parseInt(id));
		Object query_ret = client.execute("g.v(ID).map();", param);
		List<Map<String, Object>> query_ret_list = (List<Map<String, Object>>)query_ret;
		Map<String, Object> query_ret_map = query_ret_list.get(0);
		return query_ret_map;
	}

	public Map<String,Object> findVert(String name) throws IOException, RexProException{
		if(name == null || name == "")
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
			logger.warn("findVert found more than 1 matching verts for name:" + name);
			return null;
		}

		return query_ret_list.get(0);
	}

	/*
    public Map<String,Object> findEdge(String edgeName) throws IOException, RexProException{
    	if(edgeName == null || edgeName == "")
    		return null;
    	Map<String, Object> param = new HashMap<String, Object>();
    	param.put("NAME", edgeName);
    	Object query_ret = client.execute("g.query().has(\"name\",NAME).edges().toList();", param);
    	List<Map<String,Object>> query_ret_list = (List<Map<String,Object>>)query_ret;
    	//logger.info("query returned: " + query_ret_list);
    	if(query_ret_list.size() == 0){
    		//logger.info("findEdge found 0 matching edges for name:" + name); //this is too noisy, the invoking function can complain if it wants to...
    		return null;
    	}else if(query_ret_list.size() > 1){
    		logger.warn("findEdge found more than 1 matching edges for name:" + edgeName);
    		return null;
    	}
    	return query_ret_list.get(0);
    }
	 */

	//function is searching vertIDCache first, if id is not in there, then it is calling the findVert funciton
	public String findVertId(String name) throws IOException, RexProException{
		String id = vertIDCache.get(name);

		//  	for (String key: vertIDCache.keySet()){
		//  		System.out.println("key = " + key + " value = " + vertIDCache.get(key));
		//  	}

		if(id != null){
			return id;
		}else{
			Map<String, Object> vert = findVert(name);
			if(vert == null) 
				id = null;
			else 
				id = (String)vert.get("_id");
			if(id != null){
				//TODO cache eviction, and/or limit caching by vert type.  But until vertex count gets higher, it won't matter much.
				vertIDCache.put(name, id);
			}
			return id;
		}
	}

	public List<Map<String,Object>> findAllVertsByType(String vertexType) throws IOException, RexProException{
		if(vertexType == null || vertexType == "")
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
		//String query = "g.query()";
		String query = "g.V";
		for(int i=0; i<constraints.size(); i++){
			Constraint c = constraints.get(i);
			String cond = c.condString(c.cond);
			String key = c.prop.toUpperCase()+i;
			param.put(key, c.val);
			query += ".has(\"" + c.prop + "\"," + cond + "," + key + ")";
		}
		//query += ".vertices().toList();";
		query += ";";
		Object query_ret = client.execute(query, param);
		List<Map<String,Object>> query_ret_list = (List<Map<String,Object>>)query_ret;

		return query_ret_list;
	}

	/*
    public String findEdgeId(String edgeName){
    	String id = null;
    	try{
    		Map<String, Object> edge = findEdge(edgeName);
    		if(edge == null) 
    			id = null;
    		else 
    			id = (String)edge.get("_id");
    		return id;
    	}catch(RexProException e){
    		logger.warn("RexProException in findEdgeId (with name: " + edgeName + " )", e);
    		return null;
    	}catch(NullPointerException e){
    		logger.error("NullPointerException in findEdgeId (with name: " + edgeName + " )", e);
    		return null;
    	}catch(IOException e){
    		logger.error("IOException in findEdgeId (with name: " + edgeName + " )", e);
    		return null;
    	}
    }
	 */

	/*
	 * @deprecated use getEdgeCount instead
	 */
	@Deprecated
	public boolean edgeExists(String inv_id, String outv_id, String label) throws RexProException, IOException {
		return (getEdgeCount(inv_id, outv_id, label) > 0);
	}
	
	/*
	 * returns edge count, or -1 if IDs not found, or ifother error occurred.
	 */
	public int getEdgeCount(String inv_id, String outv_id, String label) throws RexProException, IOException {
		int edgeCount = 0;
		if(inv_id == null || inv_id == "" || outv_id == null || outv_id == "" || label == null || label == "")
			return -1;

		Map<String, Object> param = new HashMap<String, Object>();
		param.put("ID_OUT", Integer.parseInt(outv_id));
		param.put("ID_IN", Integer.parseInt(inv_id));
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
		query_ret = client.execute("g.v(ID_OUT).outE(LABEL).inV();", param);
		
		List<Map<String, Object>> query_ret_list = (List<Map<String, Object>>)query_ret;
		//logger.info("query returned: " + query_ret_list);
		for(Map<String, Object> item : query_ret_list){
			if(Integer.parseInt(inv_id) == Integer.parseInt((String)item.get("_id")))
				edgeCount++;
		}
		//logger.info("matching edge not found");
		return edgeCount;
	}

	public void updateVert(String id, Map<String, Object> props) throws RexProException, IOException{
		String[] keys = props.keySet().toArray(new String[0]);
		for(int i=0; i<keys.length; i++){
			updateVertProperty(id, keys[i], props.get(keys[i]));
		}
	}

	public boolean updateVertProperty(String id, String key, Object val) throws RexProException, IOException{
		boolean ret = false;
		HashMap<String, Object> param = new HashMap<String, Object>();

		String cardinality = findCardinality(key);
		if(cardinality == null){
			cardinality = "SINGLE";
			cardinalityCache.put(key, cardinality);
		}

		param.put("ID", Integer.parseInt(id));
		param.put("KEY", key);

		if (cardinality.equals("SINGLE")) {
			param.put("VAL", val);
			ret = execute("g.v(ID).setProperty(KEY, VAL)", param);
		} else {
			boolean skipVal = false;
			if(val instanceof JSONArray){
				for(int i=0; i<((JSONArray)val).length(); i++){
					Object currVal = ((JSONArray)val).get(i);
					if(cardinality.equals("SET")){
						Map<String, Object> query_ret_map = getVertByID(id);
						if(query_ret_map.get(key) != null && ((List)query_ret_map.get(key)).contains(currVal)) skipVal = true;
					}
					if(!skipVal){
						param.put("VAL",currVal);
						ret = execute("g.v(ID).addProperty(KEY, VAL)", param);
					}
					skipVal = false;
				}
			}else if(val instanceof Set){
				for(Object currVal : (Set)val){
					if(cardinality.equals("SET")){
						Map<String, Object> query_ret_map = getVertByID(id);
						if(query_ret_map.get(key) != null && ((List)query_ret_map.get(key)).contains(currVal)) skipVal = true;
					}
					if(!skipVal){
						param.put("VAL", currVal);
						ret = execute("g.v(ID).addProperty(KEY, VAL)", param);
					}
					skipVal = false;
				}
			}else if(val instanceof List){
				for(Object currVal : (List)val){
					if(cardinality.equals("SET")){
						Map<String, Object> query_ret_map = getVertByID(id);
						if(query_ret_map.get(key) != null && ((List)query_ret_map.get(key)).contains(currVal)) skipVal = true;
					}
					if(!skipVal){
						param.put("VAL", currVal);
						ret = execute("g.v(ID).addProperty(KEY, VAL)", param);
					}
					skipVal = false;
				}
			}else if(val instanceof Object[]){
				for(int i=0; i<((Object[])val).length; i++){ 
					Object currVal = ((Object[])val)[i];
					if(cardinality.equals("SET")){
						Map<String, Object> query_ret_map = getVertByID(id);
						if(query_ret_map.get(key) != null && ((List)query_ret_map.get(key)).contains(currVal)) skipVal = true;
					}
					if(!skipVal){
						param.put("VAL", currVal);
						ret = execute("g.v(ID).addProperty(KEY, VAL)", param);
					}
					skipVal = false;
				}
			}else{
				if(cardinality.equals("SET")){
					Map<String, Object> query_ret_map = getVertByID(id);
					if(query_ret_map.get(key) != null && ((List)query_ret_map.get(key)).contains(val)) skipVal = true;
				}
				if(!skipVal){
					param.put("VAL", val);
					ret = execute("g.v(ID).addProperty(KEY, VAL)", param);
				}
			}
		}
		tryCommit(COMMIT_TRY_LIMIT);
		//TODO: confirm before proceeding?
		return ret;
	}
	
	/*
	public void updateEdge(String id, Map<String, Object> props) throws RexProException, IOException{
		String[] keys = props.keySet().toArray(new String[0]);
		for(int i=0; i<keys.length; i++){
			updateEdgeProperty(id, keys[i], props.get(keys[i]));
		}
	}

	//TODO
	public boolean updateEdgeProperty(String id, String key, Object val) throws RexProException, IOException{
		boolean ret = false;
		return ret;
	}
	*/
	
	/*
	 * returns cardinality of property "key".  If not found, returns null.
	 */
	public String findCardinality(String key) throws RexProException, IOException{
		String cardinality;

		cardinality = cardinalityCache.get(key);
		if(cardinality == null){
			List<Object> queryRet;

			String query = "mgmt=g.getManagementSystem();mgmt.getPropertyKey('" + key + "')";
			queryRet = client.execute(query, null);
			if(queryRet == null || queryRet.get(0) == null){
				cardinality = null;
			}else{
				query = "mgmt=g.getManagementSystem();mgmt.getPropertyKey('" + key + "').cardinality";
				queryRet = client.execute(query, null);
				cardinality = (String)queryRet.get(0);
				cardinalityCache.put(key, cardinality);
			}
		}
		return cardinality;
	}

	/*
	 * Only used by removeAllVertices()
	 */
	private boolean removeCachedVertices(){
		//NB: this query is slow enough that connection can time out if the DB starts with many vertices.

		if(vertIDCache.isEmpty())
			return true;

		boolean ret = true;
		//delete the known nodes first, to help prevent timeouts.
		Map<String,Object> param;
		Collection<String> ids = vertIDCache.values();
		for(String id : ids){
			param = new HashMap<String,Object>();
			param.put("ID", Integer.parseInt(id));
			try{
				client.execute("g.v(ID).remove();g", param);
			}catch(Exception e){
				e.printStackTrace();
				ret = false;
			}
		}
		if(ret){
			ret = tryCommit(COMMIT_TRY_LIMIT);
		}

		//clear the cache now.
		vertIDCache = new HashMap<String, String>(10000);

		return ret;

	}

	/*
	 * Only use in tests.
	 */
	public boolean removeAllVertices(){
		//NB: this query is slow enough that connection can time out if the DB starts with many vertices.
		boolean ret = false; 
		removeCachedVertices();
		try{
			client.execute("g.V.remove();g");
		}catch(Exception e){
			e.printStackTrace();
			ret = false;
		}
		try{
			int tryCount = 0;
			List<Object> queryRet;
			//Confirm before proceeding
			while(ret == false && tryCount < WRITE_CONFIRM_TRY_LIMIT){
				//System.out.println("waiting for " + tryCount + " seconds in removeAllVertices()");
				waitFor(1000*tryCount +1);
				commit();
				queryRet = client.execute("g.V.count();");
				if( (Long)queryRet.get(0) == 0){
					ret = true;
				}
				tryCount += 1;
			}
		}catch(Exception e){
			logger.warn(e.getLocalizedMessage());
			logger.warn(getStackTrace(e));
			ret = false;
		}
		return ret;
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

	/*
    public boolean removeAllEdges(RexsterClient client){
		return execute("g.E.each{g.removeVertex(it)};g.commit()");
    }*/

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
