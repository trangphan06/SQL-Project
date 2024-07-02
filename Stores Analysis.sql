Dataset: https://drive.google.com/drive/folders/1IWerRguFB0-VXLrIuHmZrfRu3O-H5WXF 

-- 1. ALTER DATA TYPES
ALTER TABLE SALES_DATASET_RFM_PRJ
ALTER COLUMN ordernumber TYPE INT USING (ordernumber::INT),
ALTER COLUMN quantityordered TYPE INT USING (quantityordered::INT),
ALTER COLUMN priceeach TYPE FLOAT USING (priceeach::FLOAT),
ALTER COLUMN orderlinenumber TYPE INT USING (orderlinenumber::INT),
ALTER COLUMN sales TYPE FLOAT USING (sales::FLOAT),
ALTER COLUMN orderdate TYPE TIMESTAMP USING (orderdate::TIMESTAMP),
ALTER COLUMN msrp TYPE INT USING (msrp::INT);

-- 2. Check NULL/BLANK (‘’): ORDERNUMBER, QUANTITYORDERED, PRICEEACH, ORDERLINENUMBER, SALES, ORDERDATE.
SELECT 
SUM(CASE WHEN CAST(ORDERNUMBER AS VARCHAR)='' THEN 1
	 WHEN ORDERNUMBER IS NULL THEN 1
	 ELSE 0 
END) AS ORDERNUMBER_NB,
SUM(CASE WHEN CAST(QUANTITYORDERED AS VARCHAR)='' THEN 1
	 WHEN QUANTITYORDERED IS NULL THEN 1
	 ELSE 0 
END) AS QUANTITYORDERED_NB,
SUM(CASE WHEN CAST(PRICEEACH AS VARCHAR)='' THEN 1
	 WHEN PRICEEACH IS NULL THEN 1
	 ELSE 0 
END) AS PRICEEACH_NB,
SUM(CASE WHEN CAST(ORDERLINENUMBER AS VARCHAR)='' THEN 1
	 WHEN ORDERLINENUMBER IS NULL THEN 1
	 ELSE 0 
END) AS ORDERLINENUMBER_NB,
SUM(CASE WHEN CAST(SALES AS VARCHAR)='' THEN 1
	 WHEN SALES IS NULL THEN 1
	 ELSE 0 
END) AS SALES_NB,
SUM(CASE WHEN CAST(ORDERDATE AS VARCHAR)='' THEN 1
	 WHEN ORDERDATE IS NULL THEN 1
	 ELSE 0 
END) AS ORDERDATE_NB
FROM SALES_DATASET_RFM_PRJ
;

-- 3. Thêm cột CONTACTLASTNAME, CONTACTFIRSTNAME được tách ra từ CONTACTFULLNAME. 
-- Chuẩn hóa CONTACTLASTNAME, CONTACTFIRSTNAME theo định dạng chữ cái đầu tiên viết hoa, chữ cái tiếp theo viết thường. 
-- Gợi ý: ( ADD column sau đó UPDATE)
ALTER TABLE sales_dataset_rfm_prj
ADD column CONTACTLASTNAME VARCHAR, 
ADD column CONTACTFIRSTNAME VARCHAR;

UPDATE sales_dataset_rfm_prj
SET CONTACTLASTNAME=CONCAT(UPPER(LEFT(CONTACTFULLNAME,1)),
						   SUBSTRING(CONTACTFULLNAME FROM 2 FOR (POSITION('-' in CONTACTFULLNAME)-2))),
	CONTACTFIRSTNAME=CONCAT(UPPER(SUBSTRING(CONTACTFULLNAME FROM (POSITION('-' IN CONTACTFULLNAME)+1) FOR 1)),
							RIGHT(CONTACTFULLNAME,LENGTH(CONTACTFULLNAME)-POSITION('-' in CONTACTFULLNAME)-1));

-- 4. Thêm cột QTR_ID, MONTH_ID, YEAR_ID lần lượt là Qúy, tháng, năm được lấy ra từ ORDERDATE 
ALTER TABLE sales_dataset_rfm_prj
ADD COLUMN QTR_ID INT,
ADD COLUMN MONTH_ID INT, 
ADD COLUMN YEAR_ID INT;

UPDATE sales_dataset_rfm_prj
SET MONTH_ID=EXTRACT(month FROM orderdate),
	YEAR_ID=EXTRACT(year FROM orderdate),
	QTR_ID=EXTRACT(quarter FROM orderdate);

-- 5. Tìm outlier (nếu có) cho cột QUANTITYORDERED và hãy chọn cách xử lý cho bản ghi đó (2 cách) ( Không chạy câu lệnh trước khi bài được review)
-- 5.1 Delete Outliers Using boxplot/IQR 
with outliers as (
with min_max_table as 
(
select 
Q1-1.5*IQR as min_value,
Q3+1.5*IQR as max_value
from 
(
select 
percentile_cont(0.25) within group (order by quantityordered) as Q1,
percentile_cont(0.75) within group (order by quantityordered) as Q3,
(percentile_cont(0.75) within group (order by quantityordered) - percentile_cont(0.25) within group (order by quantityordered))
as IQR
from sales_dataset_rfm_prj
) as IQR_table
)
select * from sales_dataset_rfm_prj
where quantityordered<(select min_value from min_max_table)
   or quantityordered>(select max_value from min_max_table)
)
delete from sales_dataset_rfm_prj
where quantityordered in(select quantityordered from outliers)   

-- 5.2 Update Outliers Using Z-SCORE = (users-avg)/stddev
with z_outliers as (
with cal_table as (
select quantityordered,
(select avg(quantityordered) from sales_dataset_rfm_prj) as avg,
(select stddev(quantityordered) from sales_dataset_rfm_prj) as stddev
from sales_dataset_rfm_prj
)
select quantityordered, 
(quantityordered-avg)/stddev as z_score
from cal_table
where abs((quantityordered-avg)/stddev)>2 -- >2 hay >3 tùy bài toán
)
UPDATE sales_dataset_rfm_prj
SET quantityordered=(select avg(quantityordered) from sales_dataset_rfm_prj) -- thay thành giá trị TB
where quantityordered in(select quantityordered from z_outliers)

-- Sau khi làm sạch dữ liệu, lưu vào bảng mới tên là SALES_DATASET_RFM_PRJ_CLEAN
CREATE TABLE SALES_DATASET_RFM_PRJ_CLEAN AS (
SELECT * FROM
(
(with cal_table as (
select quantityordered,
(select avg(quantityordered) from sales_dataset_rfm_prj) as avg,
(select stddev(quantityordered) from sales_dataset_rfm_prj) as stddev
from sales_dataset_rfm_prj
)
select quantityordered, 
(quantityordered-avg)/stddev as z_score
from cal_table
where abs((quantityordered-avg)/stddev)>2 -- >2 hay >3 tùy bài toán
)
UPDATE sales_dataset_rfm_prj
SET quantityordered=(select avg(quantityordered) from sales_dataset_rfm_prj) -- thay thành giá trị TB
where quantityordered in(select quantityordered from z_outliers)
) AS cleaned_data

--Doanh thu theo từng ProductLine, Year  và DealSize?
select productline, year_id, dealsize, sum(sales) as revenue
from public.sales_dataset_rfm_prj_clean
group by productline, year_id, dealsize;

--Đâu là tháng có bán tốt nhất mỗi năm?
select  year_id, month_id, sum(sales),
dense_rank() over(partition by year_id order by sum(sales) desc) as r
from public.sales_dataset_rfm_prj_clean
group by year_id, month_id
order by year_id;

--Product line nào được bán nhiều ở tháng 11?
select  month_id, productline, sum(sales),
dense_rank() over(partition by month_id order by sum(sales) desc) as r
from public.sales_dataset_rfm_prj_clean
group by month_id, productline
having month_id=11;

--Đâu là sản phẩm có doanh thu tốt nhất ở UK mỗi năm?
select * from (select  year_id, productline, country, sum(sales),
dense_rank() over(partition by year_id,country order by sum(sales) desc) as r
from public.sales_dataset_rfm_prj_clean
group by year_id, productline, country
having country='UK')
where r=1;

--Ai là khách hàng tốt nhất, phân tích dựa vào RFM
with cte1 as 
  (
  select c.customer_id, current_date - max(s.order_date) as r,
  count(distinct s.order_id) as f,
  sum(s.sales) as m
  from customer as c
  join sales as s on c.customer_id=s.customer_id
  group by c.customer_id
  ),
cte2 as 
  (
  select customer_id,
  ntile(5) over(order by r desc) as r_score,
  ntile(5) over(order by f desc) as f_score,
  ntile(5) over(order by m desc) as m_score
  from cte1
  ),
cte3 as 
  (
  select customer_id,
  cast(r_score as varchar)||cast(f_score as varchar)||cast(m_score as varchar) rfm_score
  from cte2
  ),
cte4 as 
  (
  select cte3.customer_id, ss.segment
  from cte3
  join segment_score as ss on cte3.rfm_score=ss.scores
  )
select * 
from cte4
where segment='Champions'
