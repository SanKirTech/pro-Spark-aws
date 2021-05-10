Begin

   merge pro-spark.retail_kpi_summary.t_revenue_country_qtr_summary s
     using pro-spark.retail_kpi.t_revenue_country_qtr_dly d
      on (s.country = d.country and s.year1 = d.year1 and s.qtr=d.qtr)
   when matched
     then update set
      s.revenue = s.revenue + d.revenue
   WHEN NOT MATCHED BY TARGET
       THEN INSERT (country, revenue, year1, qtr)
            VALUES(country, revenue, year1, qtr);

   SELECT * FROM `pro-spark.retail_kpi_summary.t_revenue_country_qtr_summary` LIMIT 10;

end