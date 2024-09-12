create database project3;
use project3;

show variables like 'secure_file_priv';

# Task 1
# create Job data table;

create table job_data(
ds date,
job_id int,
actor_id int,	
event varchar(50),
language varchar(50),
time_spent int,
org varchar(50));

Insert into job_data values('2020-11-30', 21, 1001	,'skip','English',15,'A');

Insert into job_data(ds,job_id,actor_id,event,language,time_spent,org
) values('2020-11-30', 22, 1006	,'transfer','Arabic',25,'B'),
('2020-11-29', 23, 1003	,'decision','Persian',20,'C'),
('2020-11-28', 23, 1005	,'transfer','Persian',22,'D'),
('2020-11-28', 25, 1002	,'decision','Hindi',11,'B'),
('2020-11-27', 11, 1007	,'decision','French',104,'D'),
('2020-11-26', 23, 1004	,'skip','Persian',56,'A'),
('2020-11-25', 20, 1003	,'transfer','Italian',45,'C');

select * from job_data;

select  distinct ds , sum(time_spent) over(partition by ds) as total_time_spent from job_data;

#query 1
SELECT
ds  AS Job_Date,
COUNT(job_id) AS Jobs_Per_Day,
SUM(time_spent) AS Total_Time_Spent_in_seconds,
ROUND((COUNT(job_id)*3600) / SUM(Time_Spent)/3600) AS Jobs_Reviewed_per_Hour_Day
FROM
job_data
WHERE
ds BETWEEN '2020-11-01' AND '2020-11-30'
GROUP BY Job_Date
ORDER BY Job_Date;


# query 2
SELECT
        time_spent,
        COUNT(*) AS events_per_second
    FROM
        job_data
    GROUP BY
        time_spent;
        
SELECT
    ds,
    AVG(time_spent) OVER (ORDER BY ds ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_avg_throughput
FROM
    job_data;

SELECT
    ds, throughput,
    AVG(throughput) OVER (ORDER BY ds ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) 
    AS 
    rolling_avg_events_per_second
FROM (
    SELECT ds, COUNT(*) AS throughput
    FROM job_data
    GROUP BY  ds
) AS daily_events;

 SELECT
        ds,
        COUNT(*) AS throughput
    FROM
        job_data
    GROUP BY
         ds
         order by ds;
         
#Query 3

SELECT 
    language,
    COUNT(*) * 100.0 / total_count AS percentage_share
FROM 
    job_data
WHERE 
    ds >= CURDATE() - INTERVAL 30 DAY
GROUP BY 
    language;
    
with  no_of_days as (SELECT 
    language,
    COUNT(*)  AS total_count
FROM 
    job_data
    group by language)
    
    select job_data.language, total_count, (total_count*100)/30 as percentage_share 
    from 
    job_data inner join no_of_days on job_data.language=no_of_days.language group by language ;
    
    
    # optimized version
    WITH no_of_days AS (
    SELECT 
        language,
        COUNT(*) AS total_count
    FROM 
        job_data
    GROUP BY 
        language
)
SELECT 
    language, 
    total_count, 
    (total_count * 100) / 30 AS percentage_share 
FROM 
    no_of_days;
    
    
    #more flexible one
    SET @num_days := 30;
    WITH no_of_days AS (
    SELECT 
        language,
        COUNT(*) AS total_count
    FROM 
        job_data
    WHERE 
        ds <= CURDATE() - INTERVAL @num_days DAY            -- Filter data for the specified number of days
    GROUP BY 
        language
)
SELECT 
    language, 
    total_count, 
    (total_count * 100) / @num_days AS percentage_share 
FROM 
    no_of_days ;

    
#query 4
SELECT *
FROM job_data
WHERE job_id IN (
    SELECT job_id
    FROM job_data
    GROUP BY job_id
    HAVING COUNT(*) > 1
);


SELECT * 
 FROM job_data
 GROUP BY ds, job_id, actor_id, event, language,  time_spent, org 
 HAVING COUNT(*)>1;

#task 2

#table 1 Users

create table users(
user_id int, 
created_at varchar(100),
company_id int, 
language varchar(50),
activated_at varchar(100),
state varchar(50)
);

drop table users;

select * from users;



LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users.csv'
INTO TABLE users
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS; -- Skip the header row if present

select * from users;

SET SQL_SAFE_UPDATES = 0; #This is done because updating table without where clause

#The below four lines are used to convert the created_at column into correct datetime format earlier it was incorrect.

alter table users add column temp_created_at datetime;

UPDATE users 
SET temp_created_at = STR_TO_DATE(created_at, '%d-%m-%Y %H:%i');

alter table users drop column created_at;

alter table users change column temp_created_at created_at datetime;

#Similarly doing same for column activated_at

alter table users add column temp_activated_at datetime;

UPDATE users 
SET temp_activated_at = STR_TO_DATE(activated_at, '%d-%m-%Y %H:%i');

alter table users drop column activated_at;

alter table users change column temp_activated_at activated_at datetime;


#table 2 Events
create table events (
user_id	 int,
occurred_at	varchar(100) ,
event_type	varchar(50),
event_name	varchar(100),
location	varchar(50),
device	varchar(50),
user_type int
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/events.csv'
INTO TABLE events
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS; -- Skip the header row if present




select * from events;



#Now again changing the format of occurred_at column into correct fromat of datetime.
alter table events add column temp_occurred_at datetime;

UPDATE events
SET temp_occurred_at = STR_TO_DATE(occurred_at, '%d-%m-%Y %H:%i');

alter table events drop column occurred_at;

alter table events change column temp_occurred_at occurred_at datetime;

# Creating table 3 email_events

create table email_events
(
user_id	int,
occurred_at	varchar(100),
action	varchar(100),
user_type int
);

select * from email_events;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/email_events.csv'
INTO TABLE email_events
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS; -- Skip the header row if present

select * from email_events;


#Now again changing the format of occurred_at columnof email_events into correct fromat of datetime.
alter table email_events add column temp_occurred_at datetime;

UPDATE email_events
SET temp_occurred_at = STR_TO_DATE(occurred_at, '%d-%m-%Y %H:%i');

alter table email_events drop column occurred_at;

alter table email_events change column temp_occurred_at occurred_at datetime;

#Now the tables have been imported into database .Now we will do the query one by one .

#Query 1.Write an SQL query to calculate the weekly user engagement.
select  * from  users order by user_id asc;

select  * FROM EVENTS;
select user_id, count(*) as no_of_occurrences from events group by user_id order by user_id;

SELECT 
    YEAR(occurred_at) AS year,
    WEEK(occurred_at) AS week_number,
    COUNT(*) AS total_activities
FROM 
    events
    WHERE event_type='engagement'
GROUP BY 
    YEAR(occurred_at), WEEK(occurred_at)
ORDER BY 
    YEAR(occurred_at), WEEK(occurred_at);

SELECT 
    YEAR(occurred_at) AS year,
    WEEK(occurred_at) AS week_number,
    COUNT( DISTINCT user_id) AS active_user_count
FROM 
    events
    where event_type='engagement'
GROUP BY 
    YEAR(occurred_at), WEEK(occurred_at) 
ORDER BY 
    YEAR(occurred_at), WEEK(occurred_at);
    
SELECT 
    extract(week from occurred_at)
    AS week_number,
    COUNT( DISTINCT user_id) AS active_user_count
FROM 
    events
    where event_type='engagement'
GROUP BY 
    week_number
ORDER BY 
    week_number;
    
    

#Query 2.Write an SQL query to calculate the user growth for the product.

select * from users;

select year , week_num,num_users,sum(num_users)
over (order by year,week_num) as cumulative_users
from
(select extract(year from created_at) as year,extract(week from created_at) as week_num,
count(distinct user_id) as num_users from users
group by year,week_num
order by year,week_num) sub;

select extract(created_at, '%Y-%m') AS registration_month,num_users,sum(num_users)
over (order by registration_month) as cumulative_users
from
(select DATE_FORMAT(created_at, '%Y-%m') AS registration_month,
count(distinct user_id) as num_users from users
group by registration_month
order by registration_month) sub;





SELECT
    DATE_FORMAT(created_at, '%Y-%m') AS registration_month,
    COUNT(*) AS total_users,
    (SELECT COUNT(*)
     FROM users
     WHERE DATE_FORMAT(created_at, '%Y-%m') = DATE_FORMAT(created_at, '%Y-%m')) AS cumulative_users
FROM
    users 
GROUP BY
    registration_month
ORDER BY
    registration_month;
    
    SELECT
    distinct DATE_FORMAT(created_at, '%Y-%m') AS registration_month from users;
    
    SELECT
     distinct DATE_FORMAT(occurred_at, '%Y-%m') AS registration_month from events;
     
     

SELECT COUNT(*)
     FROM users
     WHERE DATE_FORMAT(created_at, '%Y-%m') <= DATE_FORMAT(created_at, '%Y-%m') ;
    
    
select * from users where language='german';


select * from users where state='active';


#query 3.Write an SQL query to calculate the weekly retention of users based on their sign-up cohort.


select * from events where event_type='signup_flow';
select * from events;
select distinct user_id from events;

select  distinct extract(week from occurred_at ) as week_num from events;

select count(distinct user_id) as signed_users, extract(week from occurred_at)as signup_week from events 
where event_type='signup_flow' and event_name='complete_signup' group by signup_week order by signup_week;



select count(distinct user_id) as engageged_users, extract(week from occurred_at) as engagement_week 
from events
where event_type='engagement' group by engagement_week order by engagement_week;


 
 
    SELECT  distinct
        user_id AS signed_users, 
        EXTRACT(WEEK FROM occurred_at) AS signup_week 
    FROM 
        events 
    WHERE 
        event_type = 'signup_flow' AND event_name = 'complete_signup';

    SELECT  distinct
        user_id, 
        EXTRACT(WEEK FROM occurred_at) AS engagement_week 
    FROM 
        events
    WHERE 
        event_type = 'engagement';

SELECT 
    COUNT(user_id) AS total_engaged_users,
    SUM(CASE WHEN retention_week > 8 THEN 1 ELSE 0 END) AS retained_users
FROM (
    SELECT 
        a.user_id,
        a.signup_week,
        b.engagement_week,
        b.engagement_week - a.signup_week AS retention_week 
    FROM 
        cte1 a
    LEFT JOIN
        cte2 b ON a.user_id = b.user_id
) sub
GROUP BY 
    signup_week;  -- Adjust the GROUP BY clause based on your analysis needs
        
        
        
        
    WITH cte1 AS (
    SELECT DISTINCT 
        user_id AS signed_users, 
        EXTRACT(WEEK FROM occurred_at) AS signup_week 
    FROM 
        events 
    WHERE 
        event_type = 'signup_flow' AND event_name = 'complete_signup'
),
cte2 AS (
    SELECT distinct 
        user_id, 
        EXTRACT(WEEK FROM occurred_at) AS engagement_week 
    FROM 
        events
    WHERE 
        event_type = 'engagement'
)

    SELECT 
        a.signed_users,
        a.signup_week,
        b.engagement_week,
        b.engagement_week - a.signup_week AS retention_week 
    FROM 
        cte1 a
    LEFT JOIN
        cte2 b ON a.signed_users = b.user_id;

    
    
    
    

    WITH cte1 AS (
    SELECT DISTINCT 
        user_id AS signed_users, 
        EXTRACT(WEEK FROM occurred_at) AS signup_week 
    FROM 
        events 
    WHERE 
        event_type = 'signup_flow' AND event_name = 'complete_signup'
),
cte2 AS (
    SELECT distinct 
        user_id, 
        EXTRACT(WEEK FROM occurred_at) AS engagement_week 
    FROM 
        events
    WHERE 
        event_type = 'engagement'
)
SELECT 
	signup_week,
    COUNT(signed_users) AS total_signed_users,
    SUM(CASE WHEN retention_week > 0 THEN 1 ELSE 0 END) AS week1_retained_users,
    SUM(CASE WHEN retention_week > 0 THEN 1 ELSE 0 END)  / COUNT(signed_users) *100 as week1_retention_rate
   
FROM (
    SELECT 
        a.signed_users,
        a.signup_week,
        b.engagement_week,
        b.engagement_week - a.signup_week AS retention_week 
    FROM 
        cte1 a
    LEFT JOIN
        cte2 b ON a.signed_users = b.user_id
) sub
GROUP BY 
    signup_week;


#optimized version---dont write this one write above one
WITH signup_users AS (
    SELECT DISTINCT 
        user_id AS signed_users, 
        EXTRACT(WEEK FROM occurred_at) AS signup_week 
    FROM 
        events 
    WHERE 
        event_type = 'signup_flow' 
        AND event_name = 'complete_signup'
),
engagement_users AS (
    SELECT DISTINCT 
        user_id, 
        EXTRACT(WEEK FROM occurred_at) AS engagement_week 
    FROM 
        events
    WHERE 
        event_type = 'engagement'
)
SELECT 
    signup_week,
    COUNT(su.signed_users) AS total_signed_users,
    COUNT(DISTINCT CASE WHEN eu.engagement_week = signup_week THEN eu.user_id END) AS week1_retained_users,
    COUNT(DISTINCT CASE WHEN eu.engagement_week = signup_week + 1 THEN eu.user_id END) AS week2_retained_users,
    COUNT(DISTINCT CASE WHEN eu.engagement_week = signup_week + 2 THEN eu.user_id END) AS week3_retained_users
FROM 
    signup_users su
LEFT JOIN 
    engagement_users eu ON su.signed_users = eu.user_id
GROUP BY 
    signup_week;





SELECT
    cohort_week,
    sign_up_week,
    week_number,
    COUNT(DISTINCT user_id) AS active_users,
    COUNT(DISTINCT CASE WHEN week_number = 1 THEN user_id END) AS sign_up_users,
    COUNT(DISTINCT CASE WHEN week_number > 1 THEN user_id END) AS retained_users,
    COUNT(DISTINCT CASE WHEN week_number > 1 THEN user_id END) / COUNT(DISTINCT CASE WHEN week_number = 1 THEN user_id END) * 100 AS retention_rate
FROM (
    SELECT
        user_id,
        WEEK(occurred_at) AS cohort_week,
        WEEK(occurred_at) AS week_number,
        MIN(WEEK(occurred_at)) AS sign_up_week
    FROM
        events
    GROUP BY
        user_id, cohort_week
) AS cohorts
GROUP BY
    cohort_week, sign_up_week, week_number
ORDER BY
    cohort_week, sign_up_week, week_number;

#query 4.Write an SQL query to calculate the weekly engagement per device.

with device_engagement as
(select extract(week from occurred_at)as week_num,device,count(distinct user_id)as user_cnt
from events where event_type='engagement'
group by week_num,device
order by week_num)
select week_num ,device , user_cnt from device_engagement;

#optimized version
SELECT 
    EXTRACT(WEEK FROM occurred_at) AS week_num,
    device,
    COUNT(DISTINCT user_id) AS user_cnt
FROM 
    events 
WHERE 
    event_type = 'engagement'
GROUP BY 
    EXTRACT(WEEK FROM occurred_at), device
ORDER BY 
    week_num, device;




select distinct device from events;


#query 5. Write an SQL query to calculate the email engagement metrics.

select * from email_events;

select distinct user_id from email_events;

select distinct action from email_events;

select distinct user_type from email_events;
select distinct occurred_at from email_events;

SELECT
    DATE(occurred_at) AS date,
    COUNT(*) AS total_emails_sent,
    COUNT(DISTINCT user_id) AS unique_recipients,
    SUM(CASE WHEN email_cat='email_open'  THEN 1 ELSE 0 END) AS emails_opened,
    SUM(CASE WHEN email_cat='email_clicked' THEN 1 ELSE 0 END) AS emails_clicked,
    SUM(CASE WHEN email_cat='email_sent' THEN 1 ELSE 0 END) AS emails_sent
from
(select * , 
case
when action in('sent_weekly_digest','sent_reengagement_email') then 'email_sent'
when action in('email_open') then 'email_open'
when action in('email_clickthrough') then 'email_clicked'
end as email_cat
from email_events) sub

group by date;

#optimized version  

SELECT
    DATE(occurred_at) AS date,
    COUNT(*) AS total_emails_sent,
    COUNT(DISTINCT user_id) AS unique_recipients,
    SUM(CASE WHEN action = 'email_open' THEN 1 ELSE 0 END) AS emails_opened,
    SUM(CASE WHEN action = 'email_clickthrough' THEN 1 ELSE 0 END) AS emails_clicked,
    SUM(CASE WHEN action IN ('sent_weekly_digest', 'sent_reengagement_email') THEN 1 ELSE 0 END) 
    AS emails_sent
FROM
    email_events
GROUP BY
    DATE(occurred_at);













