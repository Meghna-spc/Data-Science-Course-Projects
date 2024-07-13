create database Project3;
use project3;
show databases;

#Table-1 users
create table users (
user_id int,
created_at varchar(100),
company_id int,
language varchar(50),
activated_at varchar(100),
state varchar(50));

show variables like 'secure_file_priv';

load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users.csv"
into table users
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

select * from users;

alter table users add column temp_created_at datetime;
update users set temp_created_at=STR_TO_DATE(created_at, '%d-%m-%Y %H:%i');
alter table users drop column created_at;
alter table users change column temp_created_at created_at datetime;

select * from users;

alter table users add column temp_activated_at datetime;
update users set temp_activated_at=STR_TO_DATE(activated_at, '%d-%m-%Y %H:%i');
alter table users drop column activated_at;
alter table users change column temp_activated_at activated_at datetime;

#Table-2 events

drop table events;

create table events (
user_id int,
occurred_at varchar(100),
event_type varchar(50),
event_name varchar(100),
location varchar(50),
device varchar(50),
user_type int
);

load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/events.csv"
into table events
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

alter table events add column temp_occurred_at datetime;
update events set temp_occurred_at=STR_TO_DATE(occurred_at, '%d-%m-%Y %H:%i');
alter table events drop column occurred_at;
alter table events change column temp_occurred_at occurred_at datetime;

#Table-3 email_events

create table email_events (
user_id int,
occurred_at varchar(100),
action varchar(100),
user_type int
);

load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/email_events.csv"
into table email_events
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

alter table email_events add column temp_occurred_at datetime;
update email_events set temp_occurred_at=STR_TO_DATE(occurred_at, '%d-%m-%Y %H:%i');
alter table email_events drop column occurred_at;
alter table email_events change column temp_occurred_at occurred_at datetime;

select * from email_events;

CREATE TABLE job_data (
    ds varchar(100),
    job_id INT,
    actor_id INT,
    event VARCHAR(50),
    language VARCHAR(50),
    time_spent INT,
    org VARCHAR(50)
);

load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/job_data.csv"
into table job_data
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

alter table job_data add column temp_ds date;
update job_data set temp_ds=STR_TO_DATE(ds, '%m/%d/%Y');
alter table job_data drop column ds;
alter table job_data change column temp_ds ds date;

select count(job_id)/(30*24)
as
jobs_reviewed_per_day
from
job_data;

SELECT 
  ds,
  AVG(jobs_reviewed) OVER (ORDER BY ds ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_avg_throughput
FROM (
  SELECT 
    ds,
    COUNT(distinct job_id) AS jobs_reviewed
  FROM 
    job_data
  GROUP BY 
    ds
  ORDER BY
	ds
)a;

SELECT 
  ds,
  AVG(jobs_reviewed) OVER (ORDER BY ds ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_avg_throughput
FROM (
  SELECT 
    ds,
    COUNT(job_id) AS jobs_reviewed
  FROM 
    job_data
  GROUP BY 
    ds
  ORDER BY
	ds
)a;

SELECT language, count(language),
((count(language)/(select count(*) from job_data))*100) as
percentage_share from job_data group by language;

SELECT * 
FROM 
(
SELECT *, ROW_NUMBER()OVER(PARTITION BY job_id) AS row_num
FROM job_data
) a 
WHERE row_num>1;

SELECT 
    YEARWEEK(occurred_at, 1) AS year_week_number,
    COUNT(DISTINCT user_id) AS number_of_users
FROM events
GROUP BY year_week_number
ORDER BY year_week_number;
SELECT
    user_id,
    COUNT(user_id) AS total_engagements,
    SUM(CASE WHEN retention_week = 1 THEN 1 ELSE 0 END) AS per_week_retention
FROM ( SELECT
        a.user_id,
        a.signup_week,
        b.engagement_week,
        b.engagement_week - a.signup_week AS retention_week
    FROM ( SELECT DISTINCT user_id, EXTRACT(WEEK FROM occurred_at) AS signup_week 
        FROM 
            events
        WHERE 
            event_type = 'signup_flow' AND event_name = 'complete_signup'
    ) a
    LEFT JOIN ( SELECT DISTINCT user_id, EXTRACT(WEEK FROM occurred_at) AS engagement_week 
        FROM events
        WHERE 
            event_type = 'engagement'
    ) b
    ON a.user_id = b.user_id
) d
GROUP BY user_id ORDER BY user_id;

SELECT 
    EXTRACT(YEAR FROM occurred_at) AS year,
    EXTRACT(WEEK FROM occurred_at) AS week,
    device,
    COUNT(DISTINCT user_id) AS active_users
FROM 
    events
GROUP BY 
    year, week, device
ORDER BY 
    year, week, device;
    
SELECT
    action,
    COUNT(DISTINCT user_id) AS engaged_users,
    COUNT(DISTINCT user_id) / (SELECT COUNT(DISTINCT user_id) FROM email_events) AS engagement_rate
FROM email_events
GROUP BY action;
