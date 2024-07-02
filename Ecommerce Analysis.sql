Ecommerce Dataset: Exploratory Data Analysis (EDA) and Cohort Analysis in SQL
/* I. Ad-hoc tasks
  1. Số lượng đơn hàng và số lượng khách hàng mỗi tháng
  Thống kê tổng số lượng người mua và số lượng đơn hàng đã hoàn thành mỗi tháng (Từ 1/2019-4/2022)
  Output: month_year (yyyy-mm) , total_user, total_order
  Insight là gì? ( nhận xét về sự tăng giảm theo thời gian)
*/

select
format_date('%Y-%m', created_at) as month_year,
count(distinct user_id) as total_user,
count(distinct order_id) as total_order
from bigquery-public-data.thelook_ecommerce.orders
group by month_year
order by month_year 
;

/* Insight: 
- Nhìn chung, số lượng order tăng đều theo thời gian, mỗi năm cũng tăng đều, chưa thấy seasonal trend rõ rệt. 
- Những năm đầu, số lượng total_order và total_user bằng nhau hoặc chênh nhau ít, chứng tỏ mỗi user mua hàng với số lượng lẻ tẻ, nhỏ giọt.
- Những năm gần đây, số lượng total_user lớn hơn số lượng total_order, chứng tỏ mỗi user bắt đầu mua hàng với số lượng nhiều hơn, đỉnh điểm là trong 4 tháng gần nhất.
*/

/* 
  2. Giá trị đơn hàng trung bình (AOV) và số lượng khách hàng mỗi tháng
  Thống kê giá trị đơn hàng trung bình và tổng số người dùng khác nhau mỗi tháng (Từ 1/2019-4/2022)
  Output: month_year (yyyy-mm), distinct_users, average_order_value
*/

select format_date('%Y-%m', b.created_at) as month_year,
count(distinct b.user_id) as distinct_users,
round(avg(a.sale_price*b.num_of_item),2) as average_order_value
from bigquery-public-data.thelook_ecommerce.order_items as a
join bigquery-public-data.thelook_ecommerce.orders as b
on b.order_id=a.order_id
where format_date('%Y-%m', b.created_at) < '2022-05'
group by month_year
order by month_year 

/* 
  3. Nhóm khách hàng theo độ tuổi
  Tìm các khách hàng có trẻ tuổi nhất và lớn tuổi nhất theo từng giới tính (Từ 1/2019-4/2022)
  Output: first_name, last_name, gender, age, tag (hiển thị youngest nếu trẻ tuổi nhất, oldest nếu lớn tuổi nhất)
*/
/* 
  3. Nhóm khách hàng theo độ tuổi
  Tìm các khách hàng có trẻ tuổi nhất và lớn tuổi nhất theo từng giới tính (Từ 1/2019-4/2022)
  Output: first_name, last_name, gender, age, tag (hiển thị youngest nếu trẻ tuổi nhất, oldest nếu lớn tuổi nhất)
*/
with youngest_female as 
(
select first_name, last_name, gender, age,
"youngest" as tag
from bigquery-public-data.thelook_ecommerce.users
where gender='F'
and age=(select min(age) 
         from bigquery-public-data.thelook_ecommerce.users
         where gender='F')
),
youngest_male as 
(
select first_name, last_name, gender, age,
"youngest" as tag
from bigquery-public-data.thelook_ecommerce.users
where gender='M'
and age=(select min(age) 
         from bigquery-public-data.thelook_ecommerce.users
         where gender='M')
),
oldest_female as 
(
select first_name, last_name, gender, age,
"oldest" as tag
from bigquery-public-data.thelook_ecommerce.users
where gender='F'
and age=(select max(age) 
         from bigquery-public-data.thelook_ecommerce.users
         where gender='F')
),
oldest_male as 
(
select first_name, last_name, gender, age,
"oldest" as tag
from bigquery-public-data.thelook_ecommerce.users
where gender='M'
and age=(select max(age) 
         from bigquery-public-data.thelook_ecommerce.users
         where gender='M')
),
tong_hop as 
(
select first_name, last_name, gender, age, tag
from youngest_female
UNION ALL
select first_name, last_name, gender, age, tag
from youngest_male
UNION ALL
select first_name, last_name, gender, age, tag
from oldest_female
UNION ALL
select first_name, last_name, gender, age, tag
from oldest_male
)
select gender, tag, count(*)
from tong_hop
group by gender, tag

/* Insight:
- Ở cả 2 gender, trẻ nhất đều là 12 tuổi, lớn nhất đều là 70 tuổi
- Oldest female có số lượng ít nhất là 784
- Youngest female có số lượng nhiều nhất là 876
*/

/* 
  4.Top 5 sản phẩm mỗi tháng.
  Thống kê top 5 sản phẩm có lợi nhuận cao nhất từng tháng (xếp hạng cho từng sản phẩm). 
  Output: month_year (yyyy-mm), product_id, product_name, sales, cost, profit, rank_per_month
*/
with cte as 
(
select format_date('%Y-%m', a.created_at) as month_year,
a.product_id as product_id,
b.name as product_name,
sum(a.sale_price) as sales,
sum(b.cost) as cost,
sum(a.sale_price)-sum(b.cost) as profit
from bigquery-public-data.thelook_ecommerce.order_items as a
join bigquery-public-data.thelook_ecommerce.products as b
on a.product_id=b.id
group by month_year, a.product_id, b.name
order by month_year
),
rank_table as 
(
select *,
dense_rank() over(partition by month_year order by profit) as rank_per_month
from cte
)
select *
from rank_table
where rank_per_month <6
order by month_year
;

/* 
  5.Doanh thu tính đến thời điểm hiện tại trên mỗi danh mục
  Thống kê tổng doanh thu theo ngày của từng danh mục sản phẩm (category) trong 3 tháng qua (giả sử ngày hiện tại là 15/4/2022) 
  Output: dates (yyyy-mm-dd), product_categories, revenue
*/
with cte as
(
select format_date('%Y-%m-%d', a.created_at) as dates,
b.category as product_categories,
sum(a.sale_price) as revenue
from bigquery-public-data.thelook_ecommerce.order_items as a
join bigquery-public-data.thelook_ecommerce.products as b
on a.product_id=b.id
group by dates, b.category
)
select * from cte
where PARSE_DATE('%Y-%m-%d',dates) >= date_sub('2022-04-15', interval 3 month)
and dates<='2022-04-15'
order by dates
;

/* II. Tạo metric trước khi dựng dashboard
    1. Build dataset
*/

with cte as
  (
    select 
    format_date('%Y-%m', c.created_at) as month,
    format_date('%Y', c.created_at) as year,
    b.category as product_category,
    round(sum(a.sale_price*c.num_of_item),2) as TPV,
    count(distinct c.order_id) as TPO,
    round(sum(b.cost*c.num_of_item),2) as total_cost
    from bigquery-public-data.thelook_ecommerce.order_items as a
    join bigquery-public-data.thelook_ecommerce.products as b
    on a.product_id=b.id
    join bigquery-public-data.thelook_ecommerce.orders as c
    on c.order_id=a.order_id
    group by month, year, product_category
    order by month, product_category
  ),
cte2 as
(
  select *, 
  round(100.00*(LEAD(TPV) over(partition by product_category order by month) - TPV)/TPV,2)||'%' as revenue_growth,
  round(100.00*(LEAD(TPO) over(partition by product_category order by month) - TPO)/TPO,2)||'%' as revenue_growth,
  round(TPV-total_cost,2) as total_profit,
  round((TPV-total_cost)/total_cost,2) as profit_to_cost_ratio
  from cte
)
select * from cte2

/* 2. Tạo retention cohort analysis
      Ở mỗi cohort chỉ theo dõi 3 tháng (indext từ 1 đến 4)
*/
with cte1 as 
    (
    select user_id, 
    format_date('%Y-%m',created_at) as order_date, 
    min(format_date('%Y-%m',created_at)) over(partition by user_id) as cohort_date
    from bigquery-public-data.thelook_ecommerce.orders
    ),
cte2 as 
    (
    select user_id, cohort_date,
    (extract(year from parse_date('%Y-%m',order_date))-extract(year from parse_date('%Y-%m',cohort_date)))*12 
    + (extract(month from parse_date('%Y-%m',order_date))-extract(month from parse_date('%Y-%m',cohort_date))) + 1 index
    from cte1
    ), 
cte3 as 
    (
    select cohort_date, index,
    count(distinct user_id) cnt
    from cte2
    group by cohort_date, index
    having index<=4
    order by cohort_date, index
    ),
cte4 as 
    (
    select cohort_date,
    sum(case when index=1 then cnt else 0 end) i1,
    sum(case when index=2 then cnt else 0 end) i2,
    sum(case when index=3 then cnt else 0 end) i3,
    sum(case when index=4 then cnt else 0 end) i4
    from cte3
    group by cohort_date
    )
select cohort_date,
round(100.0*i1/i1,2)||'%' as i1,
round(100.0*i2/i1,2)||'%' as i2,
round(100.0*i3/i1,2)||'%' as i3,
round(100.0*i4/i1,2)||'%' as i4
from cte4
order by cohort_date;
