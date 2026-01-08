%sql
CREATE OR REPLACE VIEW gold.gavl.gavl_consolidated_pnl AS
WITH 
aaf_cy_tb AS (
    SELECT 
        upper(Business) AS BU,
        Period_to_analyze_month_date,
        upper(Dashboard_Category) as Dashboard_Category,
        `Grouping` AS Group,
        --MIS_Sub_Grouping AS Sub_Group,
        Profit_Center_Key,
        Profit_Center_Desc,
        SUM(YTD) AS MTD
    FROM silver.md_aaf.md_consolidated_tb_cy
    WHERE `Grouping` IS NOT NULL AND Business != 'GCGPL'
    GROUP BY Business, Period_to_analyze_month_date, Dashboard_Category,
             Group, Profit_Center_Key, Profit_Center_Desc
),

aaf_ly_tb AS (
    SELECT 
        upper(Business) AS BU,
        Period_to_analyze_month_date,
        upper(Dashboard_Category) as Dashboard_Category,
        `Grouping` AS Group,
        --MIS_Sub_Grouping AS Sub_Group,
        Profit_Center_Key,
        Profit_Center_Desc,
        SUM(YTD) AS MTD
    FROM silver.md_aaf.md_consolidated_tb_ly
    WHERE `Grouping` IS NOT NULL  AND Business != 'GCGPL'
    GROUP BY Business, Period_to_analyze_month_date, Dashboard_Category,
             Group, Profit_Center_Key, Profit_Center_Desc
),

cpb_cy_tb AS (
    SELECT 
        'CPB' AS BU,
        Period_to_analyze_month_date,
        Dashboard_Category,
        `Grouping` AS Group,
        --MIS_Sub_Grouping AS Sub_Group,
        Profit_Center_Key,
        Profit_Center_Desc,
        SUM(YTD) AS MTD
    FROM silver.cpb.cpb_Md_consolidated_TB_CY 
    WHERE `Grouping` IS NOT NULL
    GROUP BY BU, Period_to_analyze_month_date, Dashboard_Category,
             Group, Profit_Center_Key, Profit_Center_Desc
),

cpb_ly_tb AS (
    SELECT 
        'CPB' AS BU,
        Period_to_analyze_month_date,
        Dashboard_Category,
        `Grouping` AS Group,
        --MIS_Sub_Grouping AS Sub_Group,
        Profit_Center_Key,
        Profit_Center_Desc,
        SUM(YTD) AS MTD
    FROM silver.cpb.cpb_Md_consolidated_TB_LY 
    WHERE `Grouping` IS NOT NULL
    GROUP BY BU, Period_to_analyze_month_date, Dashboard_Category,
             Group, Profit_Center_Key, Profit_Center_Desc
),

seeds_cy_tb AS (
    SELECT 
        'SEEDS' AS BU,
        Period_to_analyze_month_date,
        Dashboard_Category,
        `Grouping` AS Group,
        --MIS_Sub_Grouping AS Sub_Group,
        Profit_Center_Key,
        Profit_Center_Desc,
        SUM(YTD) AS MTD
    FROM silver.cpb.seeds_md_consolidated_TB_CY 
    WHERE `Grouping` IS NOT NULL
    GROUP BY BU, Period_to_analyze_month_date, Dashboard_Category,
             Group, Profit_Center_Key, Profit_Center_Desc
),

seeds_ly_tb AS (
    SELECT 
        'SEEDS' AS BU,
        Period_to_analyze_month_date,
        Dashboard_Category,
        `Grouping` AS Group,
        --MIS_Sub_Grouping AS Sub_Group,
        Profit_Center_Key,
        Profit_Center_Desc,
        SUM(YTD) AS MTD
    FROM silver.cpb.seeds_md_consolidated_TB_LY 
    WHERE `Grouping` IS NOT NULL
    GROUP BY BU, Period_to_analyze_month_date, Dashboard_Category,
             Group, Profit_Center_Key, Profit_Center_Desc
),

opb_cy_tb AS (
    SELECT 
        'OPB' AS BU,
        Period_to_analyze_month_date,
        upper(Dashboard_Category) as Dashboard_Category,
        `Grouping` AS Group,
        --MIS_Sub_Grouping AS Sub_Group,
        Profit_Center_Key,
        Profit_Center_Desc,
        SUM(YTD) AS MTD
    FROM silver.opb.md_consolidated_tb_cy
    WHERE `Grouping` IS NOT NULL
    GROUP BY BU, Period_to_analyze_month_date, Dashboard_Category,
             Group, Profit_Center_Key, Profit_Center_Desc
),

opb_ly_tb AS (
    SELECT 
        'OPB' AS BU,
        Period_to_analyze_month_date,
        upper(Dashboard_Category) as Dashboard_Category,
        `Grouping` AS Group,
        --MIS_Sub_Grouping AS Sub_Group,
        Profit_Center_Key,
        Profit_Center_Desc,
        SUM(YTD) AS MTD
    FROM silver.opb.md_consolidated_tb_ly
    WHERE `Grouping` IS NOT NULL
    GROUP BY BU, Period_to_analyze_month_date, Dashboard_Category,
             Group, Profit_Center_Key, Profit_Center_Desc
),

gavl_cy_tb AS (
    SELECT
        upper(BU) as BU,
        Period_to_analyze_month_date,
        upper(Dashboard_Category) as Dashboard_Category,
        `Grouping` as Group,
        --MIS_Sub_Grouping as Sub_Group,
        Profit_Center_Key,
        Profit_Center_Desc,
        SUM(YTD) as MTD
    FROM silver.gavl.gavl_tb_cy
    WHERE `Grouping` is not NULL
    GROUP BY BU, Period_to_analyze_month_date, Dashboard_Category,
             Group, Profit_Center_Key, Profit_Center_Desc       
),

gavl_ly_tb AS (
    SELECT
        upper(BU) as BU,
        Period_to_analyze_month_date,
        upper(Dashboard_Category) as Dashboard_Category,
        `Grouping` as Group,
        --MIS_Sub_Grouping as Sub_Group,
        Profit_Center_Key,
        Profit_Center_Desc,
        SUM(YTD) as MTD
    FROM silver.gavl.gavl_tb_ly
    WHERE `Grouping` is not NULL
    GROUP BY BU, Period_to_analyze_month_date, Dashboard_Category,
             Group, Profit_Center_Key, Profit_Center_Desc       
),

 aaf_sales AS (
        SELECT 
            upper(BU) as BU,
            Period_to_analyze_month_date,
            Target_Type,
            Division,
            SUM(CASE WHEN Uom IN ('MT', 'INR', 'LACS','KG', '') THEN Net_sales ELSE 0 END) / 10000000 AS Sales,
            SUM(Contribution) / 10000000 AS Contribution
        FROM gold.md_aaf.zaaf_sales_view
        WHERE Target_Type IN ('AOP', 'FORECAST', 'ACTUALS')
        GROUP BY BU, Period_to_analyze_month_date, Target_Type, Division
    ),

aaf_expense as (
    select 
           upper(BU) as BU,
           Period_to_analyze_month_date,
           Target_Type,
           sum(case when Expense_Type = 'DISCOUNTS' then MTD/10000000 else 0 end) as Discounts,
           sum(case when Expense_Type = 'MISCELLANEOUS SALE' then MTD/10000000 else 0 end) as Misc_Sales,
           sum(case when Expense_Type = 'RAW MATERIAL COST' then MTD/10000000 else 0 end) as RMC,
           sum(case when Expense_Type = 'TOTAL FIXED EXPENSES' then MTD/10000000 else 0 end) as Total_Fixed_Expenses,
           sum(case when Expense_Type = 'DEPRECIATION' then MTD/10000000 else 0 end) as Depreciation,
           sum(case when Expense_Type = 'VARIABLE INTEREST' then MTD/10000000 else 0 end) as Variable_Interest,
           sum(case when Expense_Type = 'FIXED INTEREST' then MTD/10000000 else 0 end) as Fixed_Interest
    from gold.md_aaf.zaaf_expense_view
    where Target_Type in ('AOP', 'FORECAST','ACTUALS')
      and Expense_Type in ('DISCOUNTS', 'MISCELLANEOUS SALE', 'RAW MATERIAL COST', 'TOTAL FIXED EXPENSES', 'DEPRECIATION', 'VARIABLE INTEREST', 'FIXED INTEREST')
    group by BU, Period_to_analyze_month_date, Target_Type
),

aaf_revenue as (
    select 
          s.BU,
           s.Period_to_analyze_month_date,
           s.Target_Type,
               sum(case when s.Division not in ('Silage', 'Cotton Seed Cake') 
                        then s.Sales else 0 end)
               - coalesce(e.Discounts,0) + coalesce(e.misc_sales)
            as Net_Sales,
           sum(case when s.Division not in ('Silage', 'Cotton Seed Cake') 
                        then s.Contribution else 0 end) as Contribution,
           e.RMC as RMC,
           e.total_fixed_expenses as Total_Fixed_Expenses,
           e.depreciation as Depreciation,
           e.variable_interest as Variable_Interest,
           e.fixed_interest as Fixed_Interest
    from aaf_sales s
    left join aaf_expense e
          on s.Period_to_analyze_month_date = e.Period_to_analyze_month_date
          and s.Target_Type = e.Target_Type
          and s.BU = e.bu
    group by s.BU, s.Period_to_analyze_month_date, s.Target_Type, e.Discounts, e.misc_sales, e.rmc, e.total_fixed_expenses, e.depreciation, e.variable_interest, e.fixed_interest
),

aaf_aop_for AS (
    SELECT
        upper(BU) as BU,
        Period_to_analyze_month_date,
        'NET SALES' AS Dashboard_Category,
        '' AS Group_Name,
        Net_Sales AS MTD,
        Target_Type
    FROM aaf_revenue
    WHERE Target_Type IN ('AOP','FORECAST')

    UNION 
    
    SELECT
        upper(BU) as BU,
        Period_to_analyze_month_date,
        'CONTRIBUTION' AS Dashboard_Category,
        '' AS Group_Name,
        Contribution AS MTD,      
        Target_Type
    FROM aaf_revenue
    WHERE Target_Type IN ('AOP','FORECAST')

    UNION

    SELECT
        upper(BU) as BU,
        Period_to_analyze_month_date,
        'NET SALES' AS Dashboard_Category,
        '' AS Group_Name,
        Net_Sales AS MTD,      
        Target_Type
    FROM aaf_revenue
    WHERE Target_Type = 'ACTUALS' and BU = 'GCGPL'

    UNION

       SELECT
        upper(BU) as BU,
        Period_to_analyze_month_date,
        'CONTRIBUTION' AS Dashboard_Category,
        '' AS Group_Name,
        Contribution AS MTD,      
        Target_Type
    FROM aaf_revenue
    WHERE Target_Type = 'ACTUALS' and BU = 'GCGPL'

    UNION

    SELECT
        upper(BU) as BU,
        Period_to_analyze_month_date,
        'MATERIAL COST' AS Dashboard_Category,
        '' AS Group_Name,
         RMC AS MTD,      
        Target_Type
    FROM aaf_revenue
    WHERE Target_Type = 'ACTUALS' and BU = 'GCGPL'

    UNION

    SELECT
        upper(BU) as BU,
        Period_to_analyze_month_date,
        'TOTAL FIXED EXPENSES' AS Dashboard_Category,
        '' AS Group_Name,
         total_fixed_expenses AS MTD,      
        Target_Type
    FROM aaf_revenue
    WHERE Target_Type = 'ACTUALS' and BU in ('GCGPL')

    UNION

      SELECT
        upper(BU) as BU,
        Period_to_analyze_month_date,
        'DEPRECIATION' AS Dashboard_Category,
        '' AS Group_Name,
         depreciation AS MTD,      
        Target_Type
    FROM aaf_revenue
    WHERE Target_Type = 'ACTUALS' and BU = 'GCGPL'

    UNION

      SELECT
        upper(BU) as BU,
        Period_to_analyze_month_date,
        'VARIABLE INTEREST' AS Dashboard_Category,
        '' AS Group_Name,
         variable_interest AS MTD,      
        Target_Type
    FROM aaf_revenue
    WHERE Target_Type = 'ACTUALS' and BU = 'GCGPL'

    UNION

    SELECT
        upper(BU) as BU,
        Period_to_analyze_month_date,
        'FIXED INTEREST' AS Dashboard_Category,
        '' AS Group_Name,
         fixed_interest AS MTD,      
        Target_Type
    FROM aaf_revenue
    WHERE Target_Type = 'ACTUALS' and BU = 'GCGPL'
),

aaf_aop AS (
    SELECT
        upper(BU) as BU,
        Period_to_analyze_month_date,
        UPPER(CASE
                WHEN Expense_Type = 'RAW MATERIAL COST' then 'MATERIAL COST'
                ELSE Expense_Type
                END) 
        AS Dashboard_Category,
        '' AS Group_Name,
        (MTD/10000000) as MTD,
        Target_Type
    FROM gold.md_aaf.zaaf_expense_view
    WHERE Target_Type IN ('AOP','FORECAST')
),

cpb_aop AS (
    SELECT
        upper(Business) AS BU,
        Period_to_analyze_month_date,
        UPPER(
            CASE 
                WHEN Item_Type = 'INDAS Net Sales' THEN 'NET SALES'
                ELSE Item_Type
            END
        ) AS Dashboard_Category,
        '' AS Group,
        (Value/100) AS MTD,
        Type AS Target_Type
    FROM gold.cpb.pnl_view
    WHERE Type IN ('AOP', 'FORECAST') AND Item_Type != 'Net Sales'
    GROUP BY BU, Period_to_analyze_month_date, Dashboard_Category, Value, Target_Type
),

astec_all AS(
    SELECT 
        'ASTEC' AS BU,
         Period_to_analyze_month_date,
         UPPER(
            CASE 
                WHEN EXPENSE_TYPE = 'Sales' THEN 'NET SALES'
                WHEN EXPENSE_TYPE = 'RMC' THEN 'MATERIAL COST'
                ELSE EXPENSE_TYPE
            END
         ) as Dashboard_Category,
         '' as Group,
         (Net_Value_in_INR_lakhs/100) as MTD,
         CASE  
            WHEN TYPE = 'ACT' THEN 'ACTUALS'
            WHEN TYPE = 'CBE' THEN 'FORECAST'
            ELSE TYPE 
        END as Target_Type   
    FROM GOLD.ASTEC.PNL_VIEW
    WHERE upper(Expense_Type) not in ('DEFERRED VOLUME','DEFERRED VALUE')
),

opb_aop AS (
    SELECT 
        'OPB' AS BU,
        Period_to_analyze_month_date,
        UPPER(
            CASE
                WHEN ITEM = 'Total Sales' THEN 'NET SALES'
                WHEN ITEM = 'Contribution Margin' THEN 'CONTRIBUTION'
                ELSE ITEM
            END 
        ) AS Dashboard_Category,
        '' AS Group,
        (Volume/10000000) AS MTD,
        CASE
            WHEN Target_Type = 'ACTUAL' THEN 'ACTUALS'
            ELSE Target_Type
        END AS Target_Type
    FROM gold.opb.md_z_sales_view
    WHERE 
        (
            ITEM = 'Total Sales' 
            AND Target_Type IN ('AOP', 'FORECAST')   
        )
        OR
        (
            ITEM IN ('Contribution Margin', 'Interest on Working Capital', 'Gross Margin')
        )
),


opb_ebit as (
    SELECT
        'OPB' as BU,
        Period_to_analyze_month_date,
        upper(Expense_Type) as Dashboard_Category,
        'EBITDA' as Group,
        (MTD/10000000) as MTD,
        CASE
            WHEN Type = 'ACTUAL' then 'ACTUALS'
            ELSE Type
        END as Target_Type
    from gold.opb.z_ebitda_view
),

corp_aop AS (
    SELECT 
        upper(BU) as BU,
        Period_to_analyze_month_date,
        Particulars as Dashboard_Category,
        '' as Group,
        (Value/10000000) as MTD,
        Target_Type
    from silver.gavl.z_corporate
),

gfl_base AS (
    SELECT
        'GFL' AS BU,
        CASE 
            WHEN Date = last_day(Date) 
                THEN date_trunc('month', Date)          
            ELSE Date                                  
        END AS Period_to_analyze_month_date,
        CASE 
          WHEN Category = 'TOTAL NET SALES' THEN 'NET SALES'
          WHEN Category = 'TOTAL CONTRIBUTION' THEN 'CONTRIBUTION'
          WHEN Category = 'TOTAL GROSS MARGIN' THEN 'GROSS MARGIN'
          ELSE Category
        END AS Dashboard_Category,
        Sub_Category AS `Group`,
        Target_Type,
        MTD
    FROM (
        SELECT Date, Category, Sub_Category, 'AOP' AS Target_Type, AOP AS MTD
        FROM gold.gfl.gfl_pnl_summary_view
        UNION ALL
        SELECT Date, Category, Sub_Category, 'FORECAST', CBE
        FROM gold.gfl.gfl_pnl_summary_view
        UNION ALL
        SELECT Date, Category, Sub_Category, 'ACTUALS', MTD_Actual
        FROM gold.gfl.gfl_pnl_summary_view
    )
    WHERE sub_category in ('TOTAL CONTRIBUTION','EBITDA','TOTAL NET SALES','PBT MARGIN','PBT','EBITDA MARGIN','TOTAL GROSS MARGIN')
    and MTD > 1
),

gfl_ly AS (
    SELECT
        'GFL' AS BU,
        CASE 
            WHEN add_months(Date, -12) = last_day(add_months(Date, -12))
                THEN date_trunc('month', add_months(Date, -12))
            ELSE add_months(Date, -12)
        END AS Period_to_analyze_month_date,
         CASE 
          WHEN Category = 'TOTAL NET SALES' THEN 'NET SALES'
          WHEN Category = 'TOTAL CONTRIBUTION' THEN 'CONTRIBUTION'
          WHEN Category = 'TOTAL GROSS MARGIN' THEN 'GROSS MARGIN'
          ELSE Category
        END AS Dashboard_Category,
        Sub_Category AS `Group`,
        'ACTUALS' AS Target_Type,
        LY_Actual AS MTD
    FROM gold.gfl.gfl_pnl_summary_view
    WHERE LY_Actual IS NOT NULL AND
    sub_category in ('TOTAL CONTRIBUTION','EBITDA','TOTAL NET SALES','PBT MARGIN','PBT','EBITDA MARGIN','TOTAL GROSS MARGIN')
),

cdpl_ebit as (
  select 
    'CDPL' as BU,
    Period_to_analyze_month_date,
    upper(Fixed_Expenses) as Dashboard_Category,
    '' as Group,
    (MTD/10000000) as MTD,
    CASE 
      WHEN Type = 'ACT' then 'ACTUALS'
      WHEN Type = 'CBE' then 'FORECAST'
      ELSE Type
    END as Target_Type
  from gold.md_cdpl.zsd_ebidta_combined_view
  where Type in ('AOP', 'CBE', 'ACT')
),

cdpl_net_sales AS (
    SELECT 
        'CDPL' AS BU,
        Period_to_analyze_month_date,
        'NET SALES' AS Dashboard_Category,
        '' AS Group,
        SUM(MTD) AS MTD,
        Target_Type
    FROM (

        -- MAIN NET SALES FROM SALES VIEW
        SELECT
            Period_to_analyze_month_date,
            NRIV/10000000 AS MTD,
            CASE 
                WHEN Type='ACT' THEN 'ACTUALS'
                WHEN Type='CBE' THEN 'FORECAST'
                ELSE Type
            END AS Target_Type
        FROM gold.md_cdpl.cdpl_sales_view_temp2
        WHERE ZDIV IN ('CDPL_ALL','CDPL_All')
          AND Type != 'DEF'

        UNION ALL

        -- NET SALES FROM FEED VIEW
        SELECT
            Period_to_analyze_month_date,
            Net_Value_in_Document_Currency/10000000 AS MTD,
            CASE 
                WHEN Target_Type='ACT' THEN 'ACTUALS'
                WHEN Target_Type='CBE' THEN 'FORECAST'
                ELSE Target_Type
            END
        FROM gold.md_cdpl.cdpl_feed_view
        WHERE Fixed_Expenses IN ('SALES FEED','SALES OTHER OP INC','SALES POWER')
    )
    GROUP BY Period_to_analyze_month_date, Target_Type
),

cdpl_gm_main AS (
    SELECT
        'CDPL' AS BU,
        Period_to_analyze_month_date,
        'GROSS MARGIN' AS Dashboard_Category,
        '' AS Group,
        SUM(Gross_Margin/10000000) AS MTD,
        CASE 
            WHEN Type='ACT' THEN 'ACTUALS'
            WHEN Type='CBE' THEN 'FORECAST'
            ELSE Type
        END AS Target_Type
    FROM gold.md_cdpl.cdpl_sales_view_temp2
    WHERE ZDIV IN ('CDPL_ALL','CDPL_All')
    GROUP BY Period_to_analyze_month_date, Type
),

cdpl_gm_add AS (
    SELECT
        Period_to_analyze_month_date,
        CASE 
            WHEN Target_Type='ACT' THEN 'ACTUALS'
            WHEN Target_Type='CBE' THEN 'FORECAST'
            ELSE Target_Type 
        END AS Target_Type,
        SUM(Net_Value_in_Document_Currency/10000000) AS add_gm
    FROM gold.md_cdpl.cdpl_feed_view
    WHERE Fixed_Expenses IN ('CON-FEED','CON-OTHER OP INCOM','CON-POWER','CON-OTHERS','GROSS MARGIN-OTHER')
    GROUP BY Period_to_analyze_month_date, Target_Type
),

cdpl_con_main AS (
    SELECT
        'CDPL' AS BU,
        Period_to_analyze_month_date,
        'CONTRIBUTION' AS Dashboard_Category,
        '' AS Group,
        SUM(Contribution/10000000) AS MTD,
        CASE 
            WHEN Type='ACT' THEN 'ACTUALS'
            WHEN Type='CBE' THEN 'FORECAST'
            ELSE Type
        END AS Target_Type
    FROM gold.md_cdpl.cdpl_sales_view_temp2
    WHERE ZDIV IN ('CDPL_ALL','CDPL_All')
    GROUP BY Period_to_analyze_month_date, Type
),

cdpl_con_add AS (
    SELECT
        Period_to_analyze_month_date,
        CASE 
            WHEN Target_Type='ACT' THEN 'ACTUALS'
            WHEN Target_Type='CBE' THEN 'FORECAST'
            ELSE Target_Type 
        END AS Target_Type,
        SUM(Net_Value_in_Document_Currency/10000000) AS add_con
    FROM gold.md_cdpl.cdpl_feed_view
    WHERE Fixed_Expenses IN ('CON-FEED','CON-OTHER OP INCOM','CON-POWER','CON-OTHERS')
    GROUP BY Period_to_analyze_month_date, Target_Type
),

cdpl_material_cost AS (
    SELECT
        'CDPL' AS BU,
        Period_to_analyze_month_date,
        'MATERIAL COST' AS Dashboard_Category,
        '' AS Group,
        SUM(RM_Cost/10000000) AS MTD,
        CASE 
            WHEN Type='ACT' THEN 'ACTUALS'
            WHEN Type='CBE' THEN 'FORECAST'
            ELSE Type
        END AS Target_Type
    FROM gold.md_cdpl.cdpl_sales_view_temp2
    WHERE ZDIV IN ('CDPL_ALL','CDPL_All')
    GROUP BY Period_to_analyze_month_date, Type
),

combined AS (
    SELECT * FROM aaf_cy_tb
    UNION ALL 
    SELECT * FROM aaf_ly_tb
    UNION ALL 
    SELECT * FROM cpb_cy_tb
    UNION ALL
    SELECT * FROM cpb_ly_tb
    UNION ALL
    SELECT * FROM seeds_cy_tb
    UNION ALL
    SELECT * FROM seeds_ly_tb
    UNION ALL 
    SELECT * FROM opb_cy_tb
    UNION ALL 
    SELECT * FROM opb_ly_tb
    UNION ALL
    SELECT * FROM gavl_cy_tb
    UNION ALL
    SELECT * FROM gavl_ly_tb
),

cumulative_tb AS (
    SELECT
        BU,
        Period_to_analyze_month_date,
        Dashboard_Category,
        Group,
        MTD,
        SUM(MTD) OVER (
            PARTITION BY BU
            ORDER BY Period_to_analyze_month_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS CUMULATIVE
    FROM combined
)

SELECT
    BU,
    Period_to_analyze_month_date,
    upper(Dashboard_Category) as Dashboard_Category,
    upper(Group) as Group,
    CASE 
        WHEN SUM(MTD) > 0 THEN (SUM(MTD) * -1)
        ELSE ABS(SUM(MTD))  -- if already negative, make it positive
        END / 10000000
         AS MTD,
    --(ROUND(SUM(MTD)/10000000, 4)) AS MTD,
    'ACTUALS' as Target_Type
FROM cumulative_tb
GROUP BY BU, Period_to_analyze_month_date, Dashboard_Category, Group
UNION
select * from aaf_aop
UNION
select * from aaf_aop_for
UNION
select * from cpb_aop
UNION
select * from corp_aop
UNION 
select * from astec_all
UNION
select * from opb_aop where target_type != 'ACTUALS'
UNION
select * from opb_ebit
UNION
select 
    BU, 
    Date(Period_to_analyze_month_date) AS Period_to_analyze_month_date, 
    Dashboard_Category,
    `Group`, 
    MTD, 
    Target_Type
from gfl_base
UNION
select 
    BU, 
    Date(Period_to_analyze_month_date) AS Period_to_analyze_month_date, 
    Dashboard_Category,
    `Group`, 
    MTD, 
    Target_Type
from gfl_ly    
UNION
select * from cdpl_ebit
UNION 
SELECT BU, Period_to_analyze_month_date, Dashboard_Category, Group, MTD, Target_Type
FROM cdpl_net_sales

UNION ALL

SELECT 
    g.BU,
    g.Period_to_analyze_month_date,
    g.Dashboard_Category,
    g.Group,
    g.MTD + COALESCE(ga.add_gm,0) AS MTD,
    g.Target_Type
FROM cdpl_gm_main g
LEFT JOIN cdpl_gm_add ga
    ON g.Period_to_analyze_month_date = ga.Period_to_analyze_month_date
   AND g.Target_Type = ga.Target_Type

UNION ALL

SELECT 
    c.BU,
    c.Period_to_analyze_month_date,
    c.Dashboard_Category,
    c.Group,
    c.MTD + COALESCE(ca.add_con,0) AS MTD,
    c.Target_Type
FROM cdpl_con_main c
LEFT JOIN cdpl_con_add ca
    ON c.Period_to_analyze_month_date = ca.Period_to_analyze_month_date
   AND c.Target_Type = ca.Target_Type

UNION ALL

-- MATERIAL COST
SELECT 
    * 
FROM cdpl_material_cost

ORDER BY Period_to_analyze_month_date;
