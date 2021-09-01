Begin
merge retail_kpi_summary.t_sku_revenue_w_summary s
  using retail_kpi.t_sku_revenue_w_dly d
   on (s.stockcode = d.stockcode)
when matched
  then update set 
   s.revenue = s.revenue + d.revenue, s.ranking = 0
WHEN NOT MATCHED 
    THEN INSERT (stockcode, revenue, ranking)
         VALUES(stockcode, revenue, 0);

select * from retail_kpi_summary.t_sku_revenue_w_summary order by ranking asc;

drop table if exists retail_kpi_summary.k1atemp;

create table retail_kpi_summary.k1atemp as SELECT stockcode, revenue, ranking() over (order by revenue desc) as ranking
FROM `retail_kpi_summary.t_sku_revenue_w_summary` group by stockCode,revenue order by 3;

update retail_kpi_summary.t_sku_revenue_w_summary a
set a.ranking = b.ranking
    from `retail_kpi_summary.k1atemp` b
    where a.stockcode = b.stockcode;

select * from retail_kpi_summary.k1atemp ;

drop table if exists retail_kpi_summary.k1atemp;

end