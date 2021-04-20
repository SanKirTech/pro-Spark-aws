merge pro-spark.retail_kpi_summary.t_sku_dow_summary s
  using pro-spark.retail_kpi.t_sku_dow_dly d
   on (s.stockcode = d.stockcode and s.day_of_week = d.day_of_week and s.country=d.country)
when matched
  then update set 
   s.revenue = s.revenue + d.revenue
WHEN NOT MATCHED BY TARGET 
    THEN INSERT (stockcode, revenue, day_of_week, country)
         VALUES(stockcode, revenue, day_of_week, country)
