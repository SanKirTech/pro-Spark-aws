Begin
merge pro-spark.retail_kpi_summary.t_sku_revenue_w_summary s
  using pro-spark.retail_kpi.t_sku_revenue_w_dly d
   on (s.stockcode = d.stockcode)
when matched
  then update set 
   s.revenue = s.revenue + d.revenue, s.rank = 0
WHEN NOT MATCHED BY TARGET 
    THEN INSERT (stockcode, revenue, rank)
         VALUES(stockcode, revenue, 0);

select * from pro-spark.retail_kpi_summary.t_sku_revenue_w_summary order by rank asc;

drop table if exists retail_kpi_summary.k1atemp;

create table retail_kpi_summary.k1atemp as SELECT stockcode, revenue, rank() over (order by revenue desc) as rank
FROM `pro-spark.retail_kpi_summary.t_sku_revenue_w_summary` group by stockCode,revenue order by 3;

update pro-spark.retail_kpi_summary.t_sku_revenue_w_summary a
set a.rank = b.rank
    from `pro-spark.retail_kpi_summary.k1atemp` b
    where a.stockcode = b.stockcode;

select * from retail_kpi_summary.k1atemp ;

drop table if exists retail_kpi_summary.k1atemp;

end