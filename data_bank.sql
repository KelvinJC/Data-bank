Case Study Questions
The following case study questions include some general data exploration analysis for the nodes and transactions
before diving right into the core business questions and finishes with a challenging final request!


        -- A. Customer Nodes Exploration
-- How many unique nodes are there on the Data Bank system?
SELECT COUNT(DISTINCT node_id)
FROM customer_nodes

-- What is the number of nodes per region?
SELECT 
    region_id, 
    COUNT(DISTINCT node_id) num_node_per_region
FROM 
    customer_nodes
GROUP BY 
    region_id

-- How many customers are allocated to each region?
SELECT 
    c.region_id, 
    r.region_name, 
    COUNT( DISTINCT customer_id) num_customers
FROM 
    customer_nodes c
JOIN 
    regions r
ON 
    c.region_id = r.region_id
GROUP BY 1,2

-- How many days on average are customers reallocated to a different node?
WITH days_spent_on_node AS (SELECT *, age(end_date, start_date) num_days
FROM customer_nodes)

SELECT AVG(num_days) avg_days_before_reallocation
FROM days_spent_on_node
WHERE end_date != '9999-12-31'


-- What is the median, 80th and 95th percentile for this same reallocation days metric for each region?
WITH days_spent_on_node AS 
                            (SELECT *, age(end_date, start_date) num_days
                            FROM customer_nodes
                            WHERE end_date != '9999-12-31')

SELECT region_id, 
        PERCENTILE_DISC(0.5) WITHIN GROUP(ORDER BY num_days) AS median,
        PERCENTILE_DISC(0.8) WITHIN GROUP(ORDER BY num_days) AS eighty_percentile,
        PERCENTILE_DISC(0.95) WITHIN GROUP(ORDER BY num_days)AS ninety_five_percentile

FROM days_spent_on_node
GROUP BY region_id


        -- B. Customer Transactions

-- What is the unique count and total amount for each transaction type?
SELECT txn_type,
        COUNT(DISTINCT customer_id) num_unique_customers, -- unique count
        COUNT(customer_id) num_transactions, -- included number of transactions in query
        SUM(txn_amount) total_amount  -- total amount
FROM customer_transactions
GROUP BY txn_type

-- What is the average total historical deposit counts and amounts for all customers?
SELECT customer_id, 
		COUNT(txn_type) num_deposits, 
		SUM(txn_amount) total_amount
FROM customer_transactions
WHERE txn_type = 'deposit'
GROUP BY customer_id

-- For each month - how many Data Bank customers make more than 1 deposit and either 1 purchase or 1 withdrawal in a single month?
WITH transactions_by_customers_and_month AS (
    SELECT 
        customer_id, 
        txn_type, 
        date_trunc('month', txn_date), 
        TO_CHAR(txn_date, 'Month') months, 
        ROW_NUMBER() OVER (PARTITION BY customer_id, date_trunc('month', txn_date) 
                            ORDER BY date_trunc('month', txn_date)) r_num
    FROM 
        customer_transactions
),

string_aggregate_transaction_types as (
    select 
        customer_id, 
        date_trunc, 
        months, 
        string_agg(txn_type, ', ')
    from 
        transactions_by_customers_and_month tcm
    group by 1, 2, 3
    ),

string_contains_dep_purchase_or_withdr as (
    select 
        customer_id, 
        date_trunc, 
        months, 
        string_agg
    from 
        string_aggregate_transaction_types st
    where 
        string_agg like '%deposit%' 
    and 
        (string_agg like '%purchase%' or string_agg like '%withdrawal%')
    ),

count_number_of_deposits_in_string as (
    select 
        *, 
        array_length(string_to_array(string_agg, 'deposit'), 1) - 1 count_deposit
    from 
        string_contains_dep_purchase_or_withdr
    ),

customers_more_than_one_deposit_in_month as (
    select * 
    from 
        count_number_of_deposits_in_string
    where 
        count_deposit > 1)

select 
    date_trunc date, 
    months, 
    count(distinct customer_id) num_customers
from 
    customers_more_than_one_deposit_in_month
group by 1, 2

-- What is the closing balance for each customer at the end of the month?

with transactions_by_customer_and_month as (
    select 
        customer_id, 
        txn_type, 
        txn_date, 
        date_trunc('month', txn_date), 
        TO_CHAR(txn_date, 'Month') months, 
        max(txn_date) over (partition by customer_id, date_trunc('month', txn_date) order by date_trunc('month', txn_date)) date_last_txn_of_month,
        txn_amount,
        case when txn_type = 'purchase' or txn_type = 'withdrawal'	
            then 0 - txn_amount
            else txn_amount
            end as txn_flow							  
    from 
        customer_transactions
    ),

running_balance as (
    select 
        *, 
        sum(txn_flow) over (partition by customer_id order by txn_date) balance
    from 
        transactions_by_customer_and_month tcm),

month_table as (
    select 
        distinct date_trunc, months
    from 
        transactions_by_customer_and_month
    order by 1),

customer_month_balance as (
    select 
        rb.customer_id, 
        mt.date_trunc, 
        mt.months, 
        balance
    from 
        (select distinct customer_id from running_balance) rb
    cross join 
        month_table mt
    left join 
        running_balance rb2
    on 
        rb.customer_id = rb2.customer_id
    and 
        mt.months = rb2.months
    and 
        mt.date_trunc = rb2.date_trunc
    and 
        rb2.txn_date = rb2.date_last_txn_of_month -- picks only end of month balances
    ),

find_null_balance as (
    select 
        customer_id, 
        date_trunc, 
        months, 
        balance, 
		sum(case when balance is not null then 1 end) over (order by customer_id, date_trunc) as grp_balance
	from 
    customer_month_balance
    )

select distinct
	customer_id, 
    date_trunc, 
    months, 
    first_value(balance) over (partition by customer_id, grp_balance) as corrected_balance
from 
    find_null_balance
order by 1

-- What is the percentage of customers who increase their closing balance by more than 5%?
    -- Interpretation:
    -- Percentage of customers who have had greater than 5% increase in closing balance between any two subsequent months
with transactions_by_customer_and_month as (
	select 
		customer_id, 
		txn_type, txn_date, 
		date_trunc('month', txn_date), 
		TO_CHAR(txn_date, 'Month') months, 
		max(txn_date) over (partition by customer_id, date_trunc('month', txn_date) order by date_trunc('month', txn_date)) date_last_txn_of_month,
		txn_amount,
		case when txn_type = 'purchase' or txn_type = 'withdrawal'	
				then 0 - txn_amount
				else txn_amount
				end as txn_flow							  
	from customer_transactions),

monthly_balance as (
	select 
		*, 
		sum(txn_flow) over (partition by customer_id, date_trunc order by date_trunc) month_sum,
		sum(txn_flow) over (partition by customer_id order by txn_date) balance 
	from transactions_by_customer_and_month tcm),

month_table as (
	select distinct 
		date_trunc, 
		months
	from
		transactions_by_customer_and_month
	order by 1),

cross_joined_customers_and_months as (
	select 
		mb.customer_id, 
		mt.date_trunc, 
		mt.months, balance
	from 
		(select distinct customer_id from monthly_balance) mb
	cross join month_table mt
	left join monthly_balance mb2
	on 
		mb.customer_id = mb2.customer_id
	and 
		mt.months = mb2.months
	and 
		mt.date_trunc = mb2.date_trunc
	and 
		mb2.txn_date = mb2.date_last_txn_of_month
	),

find_null_balance as (
	select 
		customer_id, 
		date_trunc, 
		months, 
		balance, 
		sum(case when balance is not null then 1 end) 
			over 
				(order by customer_id, date_trunc) as grp_balance
	from cross_joined_customers_and_months),
				  
replace_null_balance as (
	select distinct 
		customer_id, 
		date_trunc, 
		months, 
    	first_value(balance) 
			over 
				(partition by customer_id, grp_balance) as corrected_balance
	from find_null_balance),

month_to_month_balance as (
	select 
		customer_id, 
		date_trunc, 
		months, 
		lead(months) over (partition by customer_id) lead_month, corrected_balance as balance, 
		lead(corrected_balance) over (partition by customer_id) lead_balance
	from replace_null_balance),

balance_increase as (
	select * 
	from 
		month_to_month_balance 
	where 
		(lead_balance - balance) > 0),
							
percent_increase_in_balance as (
	select *, 
		case when balance != 0 
			then ((lead_balance - balance)/balance::float) * 100 
			else ((lead_balance - balance)/(balance::float+0.00001)) * 100 
			end as percent_inc
	from 
		balance_increase
	where 
		lead_month is not null
	),

count_customers as (
	select 
		count (distinct customer_id) num_customers_with_increase , -- customer_id, months, lead_month, balance, lead_balance, ROUND(percent_inc::DECIMAL, 2) percent_increase
		(select count (distinct customer_id) from customer_transactions) num_customers 
	from 
		percent_increase_in_balance
	where 
		percent_inc > 5)

select 
	(num_customers_with_increase::float/num_customers) * 100 as percentage_of_customers
from 
	count_customers



C. Data Allocation Challenge
To test out a few different hypotheses - the Data Bank team wants to run an experiment where different groups of customers 
would be allocated data using 3 different options:

Option 1: data is allocated based off the amount of money at the end of the previous month
Option 2: data is allocated on the average amount of money kept in the account in the previous 30 days
Option 3: data is updated real-time

For this multi-part challenge question - you have been requested to generate the following data elements 
to help the Data Bank team estimate how much data will need to be provisioned for each option:

- running customer balance column that includes the impact of each transaction
- customer balance at the end of each month
- minimum, average and maximum values of the running balance for each customer

Using all of the data available - how much data would have been required for each option on a monthly basis?

--     General Assumptions:
-- Balance value equals amount of data allocated, 
-- Accounts can be overdrawn i.e. negative balances
-- data allocation is monthly 

-- Option 2: Data is allocated on the average amount of money kept in the account in the previous 30 days
--    Further Assumption:
-- average amount kept in account means average account balance within a month (not average amount of deposit made into account)

-- Option 3: Data is updated real time.
--    Further Assumptions:
-- current balance informs data allocation, 
-- running balance gives current balance, 
-- during months without transactions customers get allocated data based on current balance

with transactions_by_customer_and_month as (
	select 
		customer_id, 
		txn_type, txn_date, 
		date_trunc('month', txn_date), 
		TO_CHAR(txn_date, 'Month') months, 
		max(txn_date) over (partition by customer_id, date_trunc('month', txn_date) order by date_trunc('month', txn_date)) date_last_txn_of_month,
		txn_amount,
		case when txn_type = 'purchase' or txn_type = 'withdrawal'	
				then 0 - txn_amount
				else txn_amount
				end as txn_flow,
		row_number() over (partition by customer_id, txn_date order by txn_date) txn_per_day_num -- used to keep running_bal reflective of each transaction per day
	from customer_transactions),

monthly_balance as (
	select 
		customer_id, txn_date, txn_type, date_trunc, months, date_last_txn_of_month, txn_flow, txn_amount,  
		sum(txn_flow) over (partition by customer_id, date_trunc order by date_trunc) month_end_bal,
		sum(txn_flow) over (partition by customer_id order by txn_date) balance,
		sum(txn_flow) over (partition by customer_id order by txn_date, txn_per_day_num) running_bal

	from transactions_by_customer_and_month tcm),

month_table as (
	select distinct 
		date_trunc, 
		months
	from
		transactions_by_customer_and_month
	order by 1),
	
cross_joined_customers_and_months as (
	select 
		mb.customer_id, mt.*, txn_date, txn_type, date_last_txn_of_month, txn_amount, txn_flow, balance, month_end_bal, running_bal 
	from 
		(select distinct customer_id from monthly_balance) mb
	cross join month_table mt
	left join monthly_balance mb2
	on 
		mb.customer_id = mb2.customer_id
	and 
		mt.months = mb2.months
	and 
		mt.date_trunc = mb2.date_trunc
	),
	
find_null_balance as (
	select 
		*, 
		sum(case when month_end_bal is not null then 1 end) 
			over 
				(order by customer_id, date_trunc) as grp_mth_end_balance,
		sum(case when running_bal is not null then 1 end) 
			over 
				(order by customer_id, date_trunc, txn_date) as grp_run_balance
	from cross_joined_customers_and_months),
				  
replace_null_balance as (
	select *, 
    	first_value(month_end_bal) 
			over 
				(partition by customer_id, grp_mth_end_balance) as corrected_mth_balance,
		first_value(running_bal) 
			over 
				(partition by customer_id, grp_run_balance) as corrected_running_bal
	from find_null_balance),
	
min_max_avg_bal as (select *,
	min(corrected_running_bal) over (partition by customer_id, date_trunc) min_running_bal,
	max(corrected_running_bal) over (partition by customer_id, date_trunc) max_running_bal,
	avg(corrected_running_bal) over (partition by customer_id, date_trunc) avg_running_bal
from replace_null_balance
order by 1, 2),

full_list as (select 
			  	customer_id, 
			  	txn_date, 
			  	months, 
			  	txn_type, 
			  	txn_amount,
			  	txn_flow,
			  	running_bal,
			  	corrected_mth_balance, 
			  	corrected_running_bal, 
			  	ROUND(avg_running_bal::decimal, 2) avg_runnin_bal,
			  	case when corrected_mth_balance > 0
			  		then corrected_mth_balance
			  		else 0
			  		end as month_end_bal_data,
			  	case when corrected_running_bal > 0
					then corrected_running_bal
					else 0
					end as running_bal_data,
			  	case when avg_running_bal > 0
					then ROUND(avg_running_bal::decimal, 2)
					else 0
					end as avg_running_bal_data
from min_max_avg_bal
order by 1),

distinct_month_end_data as (
    select distinct 
        customer_id, 
        months, 
        month_end_bal_data
    from 
        full_list
    order by 
        customer_id
    ),

option_1 as (
    select 
        months, 
        sum(month_end_bal_data) mth_bal_data_alloc
    from 
        distinct_month_end_data
    group by 
        months
    ),

option_2_and_3 as (
    select 
        months, 
        sum(running_bal_data)running_bal_data_alloc, 
        sum(avg_running_bal_data) avg_bal_data_alloc
    from 
        full_list
    group by 
        months
    )

select 
    o1.months, 
    mth_bal_data_alloc, 
    running_bal_data_alloc, 
    avg_bal_data_alloc
from 
    option_1 o1
join 
    option_2_and_3 o23
on 
    o1.months = o23.months


D. Extra Challenge
Data Bank wants to try another option which is a bit more difficult to implement 
- they want to calculate data growth using an interest calculation, just like in a traditional savings account you might have with a bank.

If the annual interest rate is set at 6% and the Data Bank team wants to reward its customers by increasing their data allocation 
based off the interest calculated on a daily basis at the end of each day, how much data would be required for this option on a monthly basis?

-- Assumptions
-- interest is added to end of month balance
-- annual interest rate is divided into daily interest rate (0.06/366 days)

with transactions_by_customer_and_month as (
	select 
		customer_id, 
		txn_type, txn_date, 
		date_trunc('month', txn_date), 
		TO_CHAR(txn_date, 'Month') months, 
		max(txn_date) over (partition by customer_id, date_trunc('month', txn_date) order by date_trunc('month', txn_date)) date_last_txn_of_month,
		txn_amount,
		case when txn_type = 'purchase' or txn_type = 'withdrawal'	
				then 0 - txn_amount
				else txn_amount
				end as txn_flow,
		row_number() over (partition by customer_id, txn_date order by txn_date) txn_per_day_num -- used to keep running_bal reflective of each transaction per day
	from customer_transactions),

monthly_balance as (
	select 
		customer_id, txn_date, txn_type, date_trunc, months, date_last_txn_of_month, txn_flow, txn_amount,  
		sum(txn_flow) over (partition by customer_id, date_trunc order by date_trunc) month_end_bal,
		sum(txn_flow) over (partition by customer_id order by txn_date) balance,
		sum(txn_flow) over (partition by customer_id order by txn_date, txn_per_day_num) running_bal

	from transactions_by_customer_and_month tcm),

month_table as (
	select distinct 
		date_trunc, 
		months
	from
		transactions_by_customer_and_month
	order by 1),
	
cross_joined_customers_and_months as (
	select 
		mb.customer_id, mt.*, txn_date, txn_type, date_last_txn_of_month, txn_amount, txn_flow, balance, month_end_bal, running_bal 
	from 
		(select distinct customer_id from monthly_balance) mb
	cross join month_table mt
	left join monthly_balance mb2
	on 
		mb.customer_id = mb2.customer_id
	and 
		mt.months = mb2.months
	and 
		mt.date_trunc = mb2.date_trunc
	),
	
find_null_balance as (
	select 
		*, 
		sum(case when month_end_bal is not null then 1 end) 
			over 
				(order by customer_id, date_trunc) as grp_mth_end_balance,
		sum(case when running_bal is not null then 1 end) 
			over 
				(order by customer_id, date_trunc, txn_date) as grp_run_balance
	from cross_joined_customers_and_months),
				  
replace_null_balance as (
	select *, 
    	first_value(month_end_bal) 
			over 
				(partition by customer_id, grp_mth_end_balance) as corrected_mth_balance,
		first_value(running_bal) 
			over 
				(partition by customer_id, grp_run_balance) as corrected_running_bal
	from find_null_balance),
	
min_max_avg_bal as (select *,
	min(corrected_running_bal) over (partition by customer_id, date_trunc) min_running_bal,
	max(corrected_running_bal) over (partition by customer_id, date_trunc) max_running_bal,
	avg(corrected_running_bal) over (partition by customer_id, date_trunc) avg_running_bal
from replace_null_balance
order by 1, 2),

full_list as (
    select 
        customer_id, 
        txn_date, 
        months,
        date_trunc,
        txn_type, 
        txn_amount,
        txn_flow,
        running_bal,
        corrected_mth_balance, 
        corrected_running_bal, 
        ROUND(avg_running_bal::decimal, 2) avg_running_bal,
        case when corrected_mth_balance > 0
            then corrected_mth_balance
            else 0
            end as month_end_bal_data,
        case when corrected_running_bal > 0
            then corrected_running_bal
            else 0
            end as running_bal_data,
        case when avg_running_bal > 0
            then ROUND(avg_running_bal::decimal, 2)
            else 0
            end as avg_running_bal_data
    from min_max_avg_bal
    order by 1),

get_daily_interest as (SELECT
	customer_id,
	months,
	date_trunc,
	corrected_mth_balance,
	case when corrected_mth_balance > 0 
		then corrected_mth_balance 
		else 0 
		end as data_alloc,
    DATE_PART('days', 
        DATE_TRUNC('month', date_trunc) + '1 MONTH'::INTERVAL - '1 DAY'::INTERVAL
		) num_days_in_month,
	ROUND((0.06 / 366)::decimal, 5) as daily_interest
from full_list),

allocation as (
    select *, 
        data_alloc + (data_alloc * (.06/366) * num_days_in_month) adjusted_data_alloc
    from 
        get_daily_interest
    )

select 
    months, 
    ROUND(SUM(adjusted_data_alloc)::decimal, 2)
from 
    allocation
group by 
    months


Special notes:

Data Bank wants an initial calculation which does not allow for compounding interest, 
however they may also be interested in a daily compounding interest calculation
so you can try to perform this calculation if you have the stamina!


Extension Request
The Data Bank team wants you to use the outputs generated from the above sections to create a quick Powerpoint presentation 
which will be used as marketing materials for both external investors who might want to buy Data Bank shares 
and new prospective customers who might want to bank with Data Bank.

    1. Using the outputs generated from the customer node questions, generate a few headline insights 
    which Data Bank might use to market its world-leading security features to potential investors and customers.

    2. With the transaction analysis - prepare a 1 page presentation slide which contains all the relevant information about the various options 
    for the data provisioning so the Data Bank management team can make an informed decision.



