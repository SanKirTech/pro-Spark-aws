
merge pro-spark.retail_kpi.t_sku_revenue_c_summary s
  using pro-spark.retail_kpi.t_sku_revenue_c_dly d
   on (s.stockcode = d.stockcode)
when matched
  then update set 
   s.revenue = s.revenue + d.revenue, s.rank = 0
WHEN NOT MATCHED BY TARGET 
    THEN INSERT (stockcode, revenue, rank)
         VALUES(stockcode, revenue, 0);

select * from pro-spark.retail_kpi.t_sku_revenue_c_summary ;

drop table if exists retail_kpi.k1btemp;

create table retail_kpi.kpi5temp as SELECT stockcode, revenue, rank() over (order by revenue desc) as rank
FROM `pro-spark.retail_kpi.t_sku_revenue_c_summary` group by stockCode,revenue order by 3;     

update pro-spark.retail_kpi.t_sku_revenue_c_summary a 
set a.rank = b.rank
    from `pro-spark.retail_kpi.k1btemp`b
    where a.stockcode = b.stockcode;

select * from retail_kpi.k1btemp ; 

drop table if exists retail_kpi.k1btemp;