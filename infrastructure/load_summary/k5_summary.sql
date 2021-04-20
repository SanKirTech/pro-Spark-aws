Begin
     
merge pro-spark.retail_kpi.t_customer_rank_summary s
  using pro-spark.retail_kpi.t_customer_rank_dly d
   on (s.customerid = d.customerid)
when matched
  then update set 
   s.revenue = s.revenue + d.revenue,s.rank = 0
WHEN NOT MATCHED BY TARGET 
    THEN INSERT (customerid, revenue, rank)
         VALUES(customerid, revenue, 0);

drop table retail_kpi.kpi5temp;

create table retail_kpi.kpi5temp as SELECT customerid, revenue, rank() over (order by revenue desc) as rank
FROM `pro-spark.retail_kpi.t_customer_rank_summary` group by customerid,revenue order by 3; 

update pro-spark.retail_kpi.t_customer_rank_summary a 
set rank = b.rank
    from `pro-spark.retail_kpi.kpi5temp`b
    where a.customerid = b.customerid;

select * from pro-spark.retail_kpi.t_customer_rank_summary order by rank;

drop table retail_kpi.kpi5temp;

end