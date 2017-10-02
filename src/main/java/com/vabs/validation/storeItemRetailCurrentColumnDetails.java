/**
 * 
 */
package com.vabs.validation;

import java.math.BigDecimal;
import java.sql.Timestamp;

/**
 * @author v0b003r
 *
 */
public class storeItemRetailCurrentColumnDetails {
	private Integer item_nbr;
	private Integer store_nbr;
	private Timestamp effective_dt;
	private Timestamp expiration_dt;
	private BigDecimal ho_rcmd_rtl;
	private String ho_rcmd_rtl_type;
	public Integer getItem_nbr() {
		return item_nbr;
	}
	public void setItem_nbr(Integer item_nbr) {
		this.item_nbr = item_nbr;
	}
	public Integer getStore_nbr() {
		return store_nbr;
	}
	public void setStore_nbr(Integer store_nbr) {
		this.store_nbr = store_nbr;
	}
	public Timestamp getEffective_dt() {
		return effective_dt;
	}
	public void setEffective_dt(Timestamp effective_dt) {
		this.effective_dt = effective_dt;
	}
	public Timestamp getExpiration_dt() {
		return expiration_dt;
	}
	public void setExpiration_dt(Timestamp expiration_dt) {
		this.expiration_dt = expiration_dt;
	}
	public BigDecimal getHo_rcmd_rtl() {
		return ho_rcmd_rtl;
	}
	public void setHo_rcmd_rtl(BigDecimal ho_rcmd_rtl) {
		this.ho_rcmd_rtl = ho_rcmd_rtl;
	}
	public String getHo_rcmd_rtl_type() {
		return ho_rcmd_rtl_type;
	}
	public void setHo_rcmd_rtl_type(String ho_rcmd_rtl_type) {
		this.ho_rcmd_rtl_type = ho_rcmd_rtl_type;
	}
	

}
