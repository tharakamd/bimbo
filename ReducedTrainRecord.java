import java.io.Serializable;

public class ReducedTrainRecord implements Serializable {
	private int sales_depot_id;
	private int client_id;
	private int product_id;
	private int sales_units;
	private int return_units;
	private int demand;
	public int getSales_depot_id() {
		return sales_depot_id;
	}
	public void setSales_depot_id(int sales_depot_id) {
		this.sales_depot_id = sales_depot_id;
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
	public int getReturn_units() {
		return return_units;
	}
	public void setReturn_units(int return_units) {
		this.return_units = return_units;
	}
	public int getDemand() {
		return demand;
	}
	public void setDemand(int demand) {
		this.demand = demand;
	}
	
	
}
