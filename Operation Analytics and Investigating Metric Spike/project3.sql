create database project3;
use project3;

create table users(
	user_id int,
    created_at varchar(100),
    company_id int,
    language varchar(50),
    activated_at varchar(50),
    state varchar(50)
);

show variables like 'secure_file_priv';
load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users.csv"
into table users
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

select * from users;
alter table users add column temp_created_at datetime;

update users set temp_created_at = str_to_date(created_at,'%d-%m-%Y %H:%i');
alter table users drop column created_at;
alter table users change column temp_created_at created_id datetime;
						
-- table 2
create table events(
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

desc events;
select * from events;

alter table events add column temp_occurred_at datetime;
update events set temp_occurred_at=str_to_date(occurred_at,'%d-%m-%Y %H:%i');
alter table events drop column occurred_at;
alter table events change column temp_occurred_at occurred_at datetime;

-- 		table 3	

create table email_events(
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

select * from email_events;
alter table email_events add column temp_occurred_at datetime;


update email_events set temp_occurred_at=str_to_date(occurred_at,'%d-%m-%Y %H:%i');
alter table email_events drop column occurred_at;
alter table email_events change column temp_occurred_at occurred_at datetime;


--  table 4 						

create table job_data(
ds varchar(100),
job_id int,
actor_id int,
event varchar(100),
language varchar(100),
time_spent int,
org varchar(2)
);
drop table job_data;
load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/job_data.csv"
into table job_data
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

alter table job_data add column temp_occurred_at date;

update job_data 
set temp_occurred_at = str_to_date(ds, '%m/%d/%Y');
ALTER TABLE job_data DROP COLUMN ds;

ALTER TABLE job_data CHANGE COLUMN temp_occurred_at ds DATE;

select * from job_data;






















-- 1 
SELECT 
    DATE(ds) AS review_date,
    HOUR(ds) AS review_hour,
    COUNT(job_id) AS jobs_reviewed
FROM 
    job_data
WHERE 
    ds >= '2020-11-01' AND ds < '2020-12-01'
GROUP BY 
    DATE(ds), HOUR(ds)
ORDER BY 
    review_date, review_hour;

-- 2
WITH event_counts AS (
    SELECT 
        DATE(ds) AS event_date,
        COUNT(event) AS event_count
    FROM 
        job_data
    GROUP BY 
        DATE(ds)
),
throughput AS (
    SELECT 
        event_date,
        event_count,
        event_count / 86400.0 AS events_per_second  -- 86400 seconds in a day
    FROM 
        event_counts
)
SELECT 
    event_date,
    events_per_second,
    AVG(events_per_second) OVER (ORDER BY event_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_avg_7_days
FROM 
    throughput;


-- 3
WITH last_30_days AS (
    SELECT 
        language,
        COUNT(*) AS language_count
    FROM 
        job_data
    WHERE 
        ds >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
    GROUP BY 
        language
),
total_jobs AS (
    SELECT 
        SUM(language_count) AS total_count
    FROM 
        last_30_days
)
SELECT 
    l.language,
    (l.language_count / t.total_count) * 100 AS language_percentage
FROM 
    last_30_days l,
    total_jobs t;
 

-- 4
SELECT 
    job_id,
    actor_id,
    event,
    language,
    time_spent,
    org,
    ds,
    COUNT(*) AS count
FROM 
    job_data
GROUP BY 
    job_id, actor_id, event, language, time_spent, org, ds
HAVING 
    COUNT(*) > 1;

