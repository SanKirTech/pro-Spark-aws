Begin
merge pro-spark.retail_kpi_summary.t_sku_revenue_c_summary s
  using pro-spark.retail_kpi.t_sku_revenue_c_dly d
   on (s.stockcode = d.stockcode and s.country = d.country)
when matched
  then update set 
   s.revenue = s.revenue + d.revenue, s.rank = 0
WHEN NOT MATCHED BY TARGET 
    THEN INSERT (stockcode, revenue, rank)
         VALUES(stockcode, revenue, 0);

select * from pro-spark.retail_kpi_summary.t_sku_revenue_c_summary ;

drop table if exists retail_kpi_summary.k1btemp;

create table retail_kpi_summary.k1btemp as SELECT stockcode, country, revenue, rank() over (order by revenue desc) as rank
FROM `pro-spark.retail_kpi_summary.t_sku_revenue_c_summary` group by stockCode,country, revenue ;

update pro-spark.retail_kpi_summary.t_sku_revenue_c_summary a
set a.rank = b.rank
    from `pro-spark.retail_kpi_summary.k1btemp` b
    where a.stockcode = b.stockcode and a.country = b.country;

select * from retail_kpi_summary.k1btemp ; 

drop table if exists retail_kpi_summary.k1btemp;

end