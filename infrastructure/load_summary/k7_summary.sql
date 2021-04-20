merge pro-spark.retail_kpi_summary.t_sales_anomaly_summary s
  using pro-spark.retail_kpi.t_sales_anomaly_dly d
   on (s.stockcode = d.stockcode and s.year1 = d.year1 and s.month1 = d.month1)
when matched
  then update set 
   s.revenue = s.revenue + d.revenue
WHEN NOT MATCHED BY TARGET 
    THEN INSERT (stockcode, year1, month1, revenue)
         VALUES(stockcode, year1, month1, revenue)
