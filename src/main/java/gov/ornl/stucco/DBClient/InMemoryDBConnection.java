package gov.ornl.stucco.DBClient;

import gov.ornl.stucco.DBClient.Constraint;
import gov.ornl.stucco.DBClient.Constraint.Condition;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.commons.io.IOUtils;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InMemoryDBConnection {

	private Logger logger = null;

	private Map<String, Map<String, Object>> vertices = null;
	private Map<String, String> vertIDs = null;
	private Map<String, Map<String, Object>> edges = null; //TODO: make/use an Edge class, to store inV, outV, label?  And maybe index that.
	//private Map<String, String> edgeIDs = null; //edges don't have meaningful names.
	private Set<String> indexedVertFields = null; //TODO: not maintaining any indexed fields for now, add later if desired.

	public InMemoryDBConnection(){
		vertices = new HashMap<String, Map<String, Object>>();
		vertIDs = new HashMap<String, String>();
		edges = new HashMap<String, Map<String, Object>>();
		//edgeIDs = new HashMap<String, String>(); //edges don't have meaningful names.
		indexedVertFields = new HashSet();
		//TODO: initialize any indexes.
	}

	public Map<String, Object> getVertByID(String vertID){
		return vertices.get(vertID);
	}

	public Map<String,Object> getVertByName(String vertName) throws InvalidStateException{
		if(vertName == null || vertName == "")
			return null;
		String id = vertIDs.get(vertName);
		if(id == null)
			return null;
		Map<String, Object> retVal = vertices.get(id);
		if(retVal == null)
			throw new InvalidStateException("bad state: known vertex name has no known content.");
		return retVal;
	}

	public String getVertIDByName(String vertName){
		if(vertName == null || vertName == "")
			return null;
		String id = vertIDs.get(vertName);
		return id;
	}
	
	
	
	public List<String> getInVertIDsByRelation(String outVertID, String relation) throws InvalidArgumentException{
		if(relation == null || relation.equals("") ){
			throw new InvalidArgumentException("cannot get edge with missing or invlid relation");
		}
		if(outVertID == null || outVertID.equals("") || !vertices.containsKey(outVertID)){
			throw new InvalidArgumentException("cannot get edge with missing or invalid outVertID");
		}

		List<String> relatedIDs = new LinkedList<String>();
		for(Map<String, Object> currEdge : edges.values()){
			if( ((String)currEdge.get("relation")).equals(relation) ){
				if( ((String)currEdge.get("outVertID")).equals(outVertID) ){
					relatedIDs.add( (String)currEdge.get("inVertID") ); //TODO: check valid state here?
				}
			}
		}
		return relatedIDs;
	}

	public List<String> getOutVertIDsByRelation(String inVertID, String relation) throws InvalidArgumentException{
		if(relation == null || relation.equals("") ){
			throw new InvalidArgumentException("cannot get edge with missing or invlid relation");
		}
		if(inVertID == null || inVertID.equals("") || !vertices.containsKey(inVertID)){
			throw new InvalidArgumentException("cannot get edge with missing or invalid inVertID");
		}

		List<String> relatedIDs = new LinkedList<String>();
		for(Map<String, Object> currEdge : edges.values()){
			if( ((String)currEdge.get("relation")).equals(relation) ){
				if( ((String)currEdge.get("inVertID")).equals(inVertID) ){
					relatedIDs.add( (String)currEdge.get("outVertID") ); //TODO: check valid state here?
				}
			}
		}
		return relatedIDs;
	}

	public List<String> getVertIDsByRelation(String vertID, String relation) throws InvalidArgumentException{
		if(relation == null || relation.equals("") ){
			throw new InvalidArgumentException("cannot get edge with missing or invlid relation");
		}
		if(vertID == null || vertID.equals("") || !vertices.containsKey(vertID)){
			throw new InvalidArgumentException("cannot get edge with missing or invalid inVertID");
		}

		List<String> relatedIDs = new LinkedList<String>();
		for(Map<String, Object> currEdge : edges.values()){
			if( ((String)currEdge.get("relation")).equals(relation) ){
				if( ((String)currEdge.get("inVertID")).equals(vertID) || ((String)currEdge.get("outVertID")).equals(vertID)){
					relatedIDs.add( (String)currEdge.get("outVertID") ); //TODO: check valid state here?
				}
			}
		}
		return relatedIDs;
	}
	
	public List<String> getEdgeIDsByVert(String inVertID, String outVertID, String relation) throws InvalidArgumentException{
		if(relation == null || relation.equals("") ){
			throw new InvalidArgumentException("cannot get edge with missing or invlid relation");
		}
		if(inVertID == null || inVertID.equals("") || !vertices.containsKey(inVertID)){
			throw new InvalidArgumentException("cannot get edge with missing or invalid inVertID");
		}
		if(outVertID == null || outVertID.equals("") || !vertices.containsKey(outVertID)){
			throw new InvalidArgumentException("cannot get edge with missing or invalid outVertID");
		}

		List<String> edgeIDs = new LinkedList<String>();
		for( String k : edges.keySet() ){
			Map<String, Object> currEdge = edges.get(k);
			if( ((String)currEdge.get("relation")).equals(relation) ){
				if( ((String)currEdge.get("inVertID")).equals(inVertID) ){
					if( ((String)currEdge.get("outVertID")).equals(outVertID) ){
						edgeIDs.add(k);
					}
				}
			}
		}
		return edgeIDs;
	}

	public List<String> getVertIDsByConstraints(List<Constraint> constraints){
		Set<String> candidateIDs = null;
		Set<String> nonMatchingIDs = new HashSet<String>();
		List<String> matchingIDs = new LinkedList<String>();

		//First, generate candidateIDs set.
		//Note that after candidateIDs is populated here, it will not be modified.
		if(indexedVertFields.size() > 0){ //TODO: indices
			//This should use indexed fields to find candidateIDs, then find the nonMatchingIDs below as usual.
			//we need to decide if only exact matches are allowed, or if ranges & etc. are ok here.
			//also, somehow indicate that the constraints used here are 'done', so they aren't re-checked below.
			candidateIDs = new HashSet<String>();
		}
		if(candidateIDs == null){ 
			//if no initial matchingIDs set was generated yet, use all IDs
			candidateIDs = vertices.keySet();
		}

		//make set of non-matching candidates, based on constraints
		for(String id : candidateIDs){
			Map<String, Object> candidateVert = vertices.get(id);
			for(Constraint c : constraints){
				if( !compare(candidateVert.get(c.prop), c.cond, c.val) ){
					nonMatchingIDs.add(id);
					break;
				}
			}
		}

		// build the matchingIDs list, based on candidateIDs and nonMatchingIDs
		for(String id : candidateIDs){
			if( !nonMatchingIDs.contains(id) ){
				matchingIDs.add(id);
			}
		}

		return matchingIDs;
	}

	private boolean compare(Object o1, Constraint.Condition cond, Object o2){

		//TODO: confirm that this is the best way to handle these cases.
		if(o1 == null && cond == Condition.eq && o2 == null)
			return true;
		if(o1 == null || o2 == null)
			return false;

		if(cond == Condition.eq){
			return o1.equals(o2);
		}
		if(cond == Condition.neq){
			return !o1.equals(o2);
		}
		if(cond == Condition.gt){
			if(o1 instanceof Comparable && o2 instanceof Comparable){
				Comparable c1 = (Comparable)o1;
				Comparable c2 = (Comparable)o2;
				return ( c1.compareTo(c2) > 0 );
			}else{
				return false;
			}
		}
		if(cond == Condition.gte){
			if(o1 instanceof Comparable && o2 instanceof Comparable){
				Comparable c1 = (Comparable)o1;
				Comparable c2 = (Comparable)o2;
				return ( c1.compareTo(c2) >= 0 );
			}else{
				return false;
			}
		}
		if(cond == Condition.lt){
			if(o1 instanceof Comparable && o2 instanceof Comparable){
				Comparable c1 = (Comparable)o1;
				Comparable c2 = (Comparable)o2;
				return ( c1.compareTo(c2) < 0 );
			}else{
				return false;
			}
		}
		if(cond == Condition.lte){
			if(o1 instanceof Comparable && o2 instanceof Comparable){
				Comparable c1 = (Comparable)o1;
				Comparable c2 = (Comparable)o2;
				return ( c1.compareTo(c2) <= 0 );
			}else{
				return false;
			}
		}
		if(cond == Condition.in){
			return contains(o1, o2);
		}
		if(cond == Condition.notin){
			return !contains(o1, o2);
		}

		return false;
	}

	private boolean contains(Object o1, Object o2){
		//TODO: confirm that all of these are behaving as a user would expect for all type combinations.
		//eg. "asdf4.222" does not contain (Double)4.2 or (Integer)4
		//[101.0, 102.0] does not contain 101, and [101, 102] does not contain 101.0
		if(o1 instanceof Collection){
			Collection c1 = (Collection)o1;
			return c1.contains(o2);
		}else if(o1 instanceof byte[]){
			byte[] a1 = (byte[])o1;
			for(int i=0; i<a1.length; i++){
				//System.out.println("val is " + a1[i]);
				if( ((Byte)a1[i]).equals(o2)) return true;
			}
		}else if(o1 instanceof short[]){
			short[] a1 = (short[])o1;
			for(int i=0; i<a1.length; i++){
				//System.out.println("val is " + a1[i]);
				if( ((Short)a1[i]).equals(o2)) return true;
			}
		}else if(o1 instanceof int[]){
			int[] a1 = (int[])o1;
			for(int i=0; i<a1.length; i++){
				//System.out.println("val is " + a1[i]);
				if( ((Integer)a1[i]).equals(o2)) return true;
			}
		}else if(o1 instanceof long[]){
			long[] a1 = (long[])o1;
			for(int i=0; i<a1.length; i++){
				//System.out.println("val is " + a1[i]);
				if( ((Long)a1[i]).equals(o2)) return true;
			}
		}else if(o1 instanceof float[]){
			float[] a1 = (float[])o1;
			for(int i=0; i<a1.length; i++){
				//System.out.println("val is " + a1[i]);
				if( ((Float)a1[i]).equals(o2)) return true;
			}
		}else if(o1 instanceof double[]){
			double[] a1 = (double[])o1;
			for(int i=0; i<a1.length; i++){
				//System.out.println("val is " + a1[i]);
				if( ((Double)a1[i]).equals(o2)) return true;
			}
		}else if(o1 instanceof boolean[]){
			boolean[] a1 = (boolean[])o1;
			for(int i=0; i<a1.length; i++){
				//System.out.println("val is " + a1[i]);
				if( ((Boolean)a1[i]).equals(o2)) return true;
			}
		}else if(o1 instanceof char[]){
			char[] a1 = (char[])o1;
			for(int i=0; i<a1.length; i++){
				//System.out.println("val is " + a1[i]);
				if( ((Character)a1[i]).equals(o2)) return true;
			}
		}else if(o1 instanceof Object[]){
			//System.out.println("Array is " + (Object[])o1);
			return Arrays.asList((Object[])o1).contains(o2);
		}else if(o1 instanceof String){
			String s1 = (String)o1;
			//System.out.println("String is " + s1);
			if(o2 instanceof CharSequence || o2 instanceof Character)
				return s1.contains(o2.toString());
			else
				return false;
		}
		return false;
	}

	public Map<String,Object> getEdgeByID(String edgeID){
		return edges.get(edgeID);
	}


	public Map<String,Object> removeEdgeByID(String edgeID){
		//TODO: update any indices
		return edges.remove(edgeID);
	}

	public Map<String,Object> removeVertByID(String vertID) throws InvalidStateException{
		Object nameObj = vertices.get(vertID).get("name");
		if(nameObj == null || !(nameObj instanceof String) ){
			throw new InvalidStateException("bad state: vertex must contain name field");
		}

		String name = (String)nameObj;
		vertIDs.remove(name);
		//TODO: update any indices
		return vertices.remove(vertID);
	}

	public String addVertex(Map<String, Object> vert) throws InvalidArgumentException, InvalidStateException{
		Object nameObj = vert.get("name");
		if(nameObj == null || !(nameObj instanceof String) || ((String)nameObj).equals("") ){
			throw new InvalidArgumentException("cannot add vertes with empty name field");
		}//TODO check any other mandatory fields

		String name = (String)nameObj;
		if(vertIDs.containsKey(name)){
			removeVertByID(getVertIDByName(name));
		}
		String vertID = String.valueOf( UUID.randomUUID() );
		vertIDs.put(name, vertID);
		vertices.put(vertID, vert);
		//TODO: update any indices
		return vertID;
	}

	public String addEdge(String inVertID, String outVertID, String relation) throws InvalidArgumentException{
		if(relation == null || relation.equals("") ){
			throw new InvalidArgumentException("cannot add edge with missing or invlid relation");
		}
		if(inVertID == null || inVertID.equals("") || !vertices.containsKey(inVertID)){
			throw new InvalidArgumentException("cannot add edge with missing or invalid inVertID");
		}
		if(outVertID == null || outVertID.equals("") || !vertices.containsKey(outVertID)){
			throw new InvalidArgumentException("cannot add edge with missing or invalid outVertID");
		}
		//TODO: check if edge is duplicate??  For now, just add it, duplicates are ok I guess.

		Map<String, Object> newEdge = new HashMap<String, Object>();
		newEdge.put("inVertID", inVertID);
		newEdge.put("outVertID", outVertID);
		newEdge.put("relation", relation);

		String edgeID = String.valueOf( UUID.randomUUID() );
		edges.put(edgeID, newEdge);
		//TODO: update any indices
		return edgeID;
	}

	public void updateVertex(String VertID, Map<String, Object> newVert) throws InvalidArgumentException{
		Map<String, Object> oldVert = vertices.get(VertID);
		if(oldVert == null){
			throw new InvalidArgumentException("invalid vertex ID");
		}
		Object newVertName = newVert.remove("name");
		Object oldVertName = oldVert.get("name");
		if(newVertName != null && !(((String)newVertName).equals((String)oldVertName)) ){
			throw new InvalidArgumentException("cannot update name of existing vertex");
		}

		for(String k: newVert.keySet()){
			oldVert.put(k, newVert.get(k));
		}
		//TODO: update any indices
	}


	private void commit(){
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

	public void save(){
		saveVertices("/tmp/stuccoDB/vertices.ser");
		saveEdges("/tmp/stuccoDB/edges.ser");
	}
	
	public void saveVertices(String filePath) {
		try {
			File file = new File(filePath);
			if (!file.exists()) {
				file.createNewFile();
			}
			FileOutputStream fileOut = new FileOutputStream(file);
			//TODO: load/save as json strings
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			if(vertices.isEmpty()) {
				out.writeObject(new JSONObject().toString());
			}else{
				out.writeObject(new JSONObject(vertices).toString());
			}
			out.close();
			fileOut.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void saveEdges(String filePath) {
		try {
			File file = new File(filePath);
			if (!file.exists()) {
				file.createNewFile();
			}
			FileOutputStream fileOut = new FileOutputStream(file);
			//TODO: load/save as json strings
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			if(edges.isEmpty()) {
				out.writeObject(new JSONObject().toString());
			}else{
				out.writeObject(new JSONObject(edges).toString());
			}
			out.close();
			fileOut.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void load(boolean reset){
		if(reset){
			vertices = new HashMap<String, Map<String, Object>>();
			vertIDs = new HashMap<String, String>();
			edges = new HashMap<String, Map<String, Object>>();
		}
		loadVertices("/tmp/stuccoDB/vertices.ser");
		loadEdges("/tmp/stuccoDB/edges.ser");
	}
	
	private void loadVertices(String filePath) {
		try {
			FileInputStream fileIn = new FileInputStream(filePath);
			//TODO: load/save as json strings
			ObjectInputStream in = new ObjectInputStream(fileIn);
			String input = in.readObject().toString();
			JSONObject json = new JSONObject(input);
			in.close();
			fileIn.close();

			for( Object id : json.keySet() ) {
				JSONObject jsonVert = json.getJSONObject(id.toString());
				Map<String, Object> vert = jsonObjectToMap(jsonVert);
				vertices.put(id.toString(), vert);
				String name = (String)vert.get("name");
				vertIDs.put(name, id.toString() );
				System.out.println("loaded vertex named " + name + " with id: " + id);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	private void loadEdges(String filePath) {
		try {
			FileInputStream fileIn = new FileInputStream(filePath);
			//TODO: load/save as json strings
			ObjectInputStream in = new ObjectInputStream(fileIn);
			String input = in.readObject().toString();
			JSONObject json = new JSONObject(input);
			in.close();
			fileIn.close();

			for( Object id : json.keySet() ) {
				JSONObject jsonEdge = json.getJSONObject(id.toString());
				Map<String, Object> edge = jsonObjectToMap(jsonEdge);
				edges.put(id.toString(), edge);
				System.out.println("loaded edge with id: " + id);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public void loadStateFromJSON(String filePath) {
		try {
			InputStream is = new FileInputStream(filePath);
	        String textContents = IOUtils.toString( is );
	        is.close();
	        
	        JSONObject contents = new JSONObject(textContents);
	        
	        JSONObject vertsJSON = contents.getJSONObject("vertices");
	        JSONArray edgesJSON = contents.getJSONArray("edges");
	        
	        //add vertices
	        for( Object id : vertsJSON.keySet() ) {
				JSONObject jsonVert = vertsJSON.getJSONObject(id.toString());
				String description = jsonVert.optString("description");
				if(description != null){
					//This is kind of an odd workaround, to prevent ui from treating, eg, "URI: www.blah.com | Type: URL |" as a URL instead of a string.
					//TODO: this is really a problem in the UI, as far as we care it's still just a string either way.
					jsonVert.put("description", " " + description);
				}else{
					//ui assumes everything has a description, this is a workaround to avoid having empty text in various places.
					jsonVert.put("description", jsonVert.optString("name"));
				}
				Map<String, Object> vert = jsonObjectToMap(jsonVert);
				vertices.put(id.toString(), vert);
				String name = (String)vert.get("name");
				vertIDs.put(name, id.toString() );
				System.out.println("loaded vertex named " + name + " with id: " + id);
			}
	        
	        //add edges.
	        for( int i=0; i<edgesJSON.length(); i++ ) {
	        	JSONObject edge = edgesJSON.getJSONObject(i);
				String edgeID;
				try {
					edgeID = addEdge(edge.getString("inVertID"), 
							edge.getString("outVertID"), 
							edge.getString("relation"));
					System.out.println("loaded edge with id: " + edgeID);
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InvalidArgumentException e) {
					// TODO Auto-generated catch block
					System.err.println("error when loading edge: " + edge);
					e.printStackTrace();
				}
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	//TODO consolidate.  Actually, should probably just make a util class for all of these.
	private Map<String, Object> jsonObjectToMap(JSONObject jsonObj) {
		Map<String, Object> ret = new HashMap<String, Object>();
		for (Object key : jsonObj.keySet()) {
			//TODO: note that this will leave all values as json-strings.  ok for now, may revisit later.
			ret.put(key.toString(), jsonObj.get(key.toString()));
		}
		return ret;
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
	
	

}
