merge `pro-spark.retail_kpi_summary.t_revenue_country_summary` s
  using `pro-spark.retail_kpi.t_revenue_country_dly` d
   on (s.country = d.country)
when matched
  then update set 
   s.revenue = s.revenue + d.revenue, s.rank = 0
WHEN NOT MATCHED BY TARGET 
    THEN INSERT (country, revenue, rank)
         VALUES(country, revenue, 0);   

drop table if exists retail_kpi_summary.k2temp;

create table retail_kpi_summary.k2temp as SELECT country, revenue, rank() over (order by revenue desc) as rank
FROM `pro-spark.retail_kpi_summary.t_revenue_country_summary` group by country,revenue order by 3;

update pro-spark.retail_kpi_summary.t_revenue_country_summary a 
set rank = b.rank
    from `pro-spark.retail_kpi_summary.k2temp`b
    where a.country = b.country;

drop table if exists retail_kpi_summary.k2temp;