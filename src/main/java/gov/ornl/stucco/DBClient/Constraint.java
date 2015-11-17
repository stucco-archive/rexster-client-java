package gov.ornl.stucco.DBClient;

public class Constraint {

	/*
	 * T.gt - greater than
	 * T.gte - greater than or equal to
	 * T.eq - equal to
	 * T.neq - not equal to
	 * T.lte - less than or equal to
	 * T.lt - less than
	 * T.in - contained in a list
	 * T.notin - not contained in a list
	 */
	public enum Condition{
		gt, gte, eq, neq, lte, lt, in, notin
	}
	public String condString(Condition c){
		if(c == Condition.eq) return "=";
		if(c == Condition.neq) return "<>";
		if(c == Condition.gt) return ">";
		if(c == Condition.gte) return ">=";
		if(c == Condition.lt) return "<";
		if(c == Condition.lte) return "<=";
		if(c == Condition.in) return "IN";
		if(c == Condition.notin) return "NOT IN";
		return null;
	}
	
	public Condition cond;
	
	public String prop;
	public Object val;
	
	public Constraint(String property, Condition condition, Object value){
		this.prop = property;
		this.cond = condition;
		this.val = value;
	}
	
}
