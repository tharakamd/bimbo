import java.io.Serializable;


public class ReducedRecordsTMP implements Serializable{
	private int week_number;
	private int sales_depot_id;
	private int sales_channel_id;
	private int route_id;
	private int client_id;
	private int product_id;
	private int sales_units;
	private float sales;
	private int return_units;
	private float returns;
	private int demand;
	public int getWeek_number() {
		return week_number;
	}
	public void setWeek_number(int week_number) {
		this.week_number = week_number;
	}
	public int getSales_depot_id() {
		return sales_depot_id;
	}
	public void setSales_depot_id(int sales_depot_id) {
		this.sales_depot_id = sales_depot_id;
	}
	public int getSales_channel_id() {
		return sales_channel_id;
	}
	public void setSales_channel_id(int sales_channel_id) {
		this.sales_channel_id = sales_channel_id;
	}
	public int getRoute_id() {
		return route_id;
	}
	public void setRoute_id(int route_id) {
		this.route_id = route_id;
	}
	public int getClient_id() {
		return client_id;
	}
	public void setClient_id(int client_id) {
		this.client_id = client_id;
	}
	public int getProduct_id() {
		return product_id;
	}
	public void setProduct_id(int product_id) {
		this.product_id = product_id;
	}
	public int getSales_units() {
		return sales_units;
	}
	public void setSales_units(int sales_units) {
		this.sales_units = sales_units;
	}
	public float getSales() {
		return sales;
	}
	public void setSales(float sales) {
		this.sales = sales;
	}
	public int getReturn_units() {
		return return_units;
	}
	public void setReturn_units(int return_units) {
		this.return_units = return_units;
	}
	public float getReturns() {
		return returns;
	}
	public void setReturns(float returns) {
		this.returns = returns;
	}
	public int getDemand() {
		return demand;
	}
	public void setDemand(int demand) {
		this.demand = demand;
	}
	
	
	
}
