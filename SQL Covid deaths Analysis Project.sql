Create database project ;

use project ;

-- First of all, we will have to load the dataset here in this database as a table done using the object explorer at the LHS of the screen. 

Select * from cov  ;  -- to see the data

-- Changing / Correcting the datatypes. 

alter table cov
alter column data_as_of date  ;

alter table cov
alter column data_period_start date ;	

alter table cov
alter column data_period_end date ;

alter table cov
alter column COVID_deaths int ;

alter table cov
alter column COVID_pct_of_total float ;

alter table cov
alter column pct_change_wk float ;

alter table cov
alter column pct_diff_wk float ;

alter table cov
alter column crude_COVID_rate float ;

alter table cov
alter column aa_COVID_rate float ;

-- Questions 1 Retrieve the jurisdiction residence with the highest number of COVID deaths for the latest data period end date.

create view q1
as	
	select jurisdiction_residence, covid_deaths, data_period_end from cov
	where covid_deaths = (select max(covid_deaths) from cov) and data_period_end = (select max(data_period_end) from cov) ;

select * from q1 ;

select * from cov
-- Question 2 Calculate the week-over-week percentage change in crude COVID rate for all jurisdictions and groups, sorted by the highest percentage change first.

select * from cov where [group] = 'weekly' ; -- to check the filter
WITH WeeklyChange AS (
    SELECT
        Jurisdiction_Residence,
        [Group],
        data_period_end,
        crude_COVID_rate,
        LAG(crude_COVID_rate) OVER (PARTITION BY Jurisdiction_Residence, [Group] ORDER BY data_period_end) AS PrevWeekRate
    FROM cov where [group] = 'weekly'
)
SELECT
    Jurisdiction_Residence,
    [Group],
    data_period_end,
    crude_COVID_rate,
    ((crude_COVID_rate - PrevWeekRate) / PrevWeekRate) * 100 AS WeekOverWeekChangePercentage
FROM
    WeeklyChange
WHERE
    PrevWeekRate != 0 and [group] = 'weekly'
ORDER BY
    WeekOverWeekChangePercentage DESC;


--Question 3: Retrieve the top 5 jurisdictions with the highest percentage difference in aa_COVID_rate compared to the overall crude COVID rate 
--            for the latest data period end date.

select top 5 jurisdiction_residence, ((aa_covid_rate - crude_covid_rate)/crude_covid_rate)*100 as 'PerDiff' from cov
where crude_covid_rate != 0 and data_period_end = (select max(data_period_end) from cov)
order by PerDiff desc  ;



--Question 4 Calculate the average COVID deaths per week for each jurisdiction residence and group, for the latest 4 data period end dates

select data_period_end, Jurisdiction_Residence , [Group], avg(covid_deaths) as 'Average Deaths per week' from cov
where data_period_end in (select distinct top 4 data_period_end from cov order by data_period_end desc) and [group] = 'weekly'
group by data_period_end, Jurisdiction_Residence , [Group]
order by data_period_end desc ;


-- Question 5 Retrieve the data for the latest data period end date, but exclude any jurisdictions that had zero COVID deaths and have missing values in any other column.

select * from cov
where data_period_end = (select max(data_period_end) from cov) and covid_deaths != 0 and pct_change_wk!= 0 and pct_diff_wk!=0
                         and COVID_pct_of_total!= 0 and crude_COVID_rate != 0 and aa_COVID_rate != 0 ;


--Question 6 Calculate the week-over-week percentage change in COVID_pct_of_total for all jurisdictions and groups, but only for the data period start dates after March 1, 2020.

WITH WeeklyChange AS (
    SELECT
        Jurisdiction_Residence,
        [Group],
        data_period_start,
        COVID_pct_of_total,
        LAG(COVID_pct_of_total) OVER (PARTITION BY Jurisdiction_Residence, [Group] ORDER BY data_period_start) AS PrevWeekPercentage
    FROM cov
    WHERE
        data_period_start > '2020-03-01' and [group] = 'weekly'
)
SELECT
    Jurisdiction_Residence,
    [Group],
    data_period_start,
    COVID_pct_of_total,
    ((COVID_pct_of_total - PrevWeekPercentage) / PrevWeekPercentage) * 100 AS WeekOverWeekChangePercentage
FROM
    WeeklyChange
WHERE
    PrevWeekPercentage != 0
ORDER BY
    Jurisdiction_Residence,
    [Group],
    data_period_start;

--Question 7 Group the data by jurisdiction residence and calculate the cumulative COVID deaths for each jurisdiction, but only up to the latest data period end date.

Select jurisdiction_residence, sum(covid_deaths) as 'cumulative deaths' from cov
where data_period_end <= (select max(data_period_end) from cov)
group by jurisdiction_residence

-- Question 8 Identify the jurisdiction with the highest percentage increase in COVID deaths from the previous week, 
--            and provide the actual numbers of deaths for each week. This would require a subquery to calculate the previous week's deaths.

WITH WeeklyChange AS (
    SELECT
        Jurisdiction_Residence,
        data_period_end,
        COVID_deaths,
        LAG(COVID_deaths) OVER (PARTITION BY Jurisdiction_Residence ORDER BY data_period_end) AS PrevWeekDeaths
    FROM cov where [group] = 'weekly'
)
SELECT
    Jurisdiction_Residence,
    data_period_end AS CurrentWeekEndDate,
    COVID_deaths AS CurrentWeekDeaths,
    PrevWeekDeaths AS PreviousWeekDeaths,
    ((COVID_deaths - PrevWeekDeaths) * 100.0 / NULLIF(PrevWeekDeaths, 0)) AS PercentageIncrease
FROM
    WeeklyChange
WHERE
    PrevWeekDeaths IS NOT NULL
ORDER BY
    PercentageIncrease DESC; 

--Question 9 Compare the crude COVID death rates for different age groups, but only for jurisdictions where the total number of deaths exceeds a certain threshold (e.g. 100). 

-- can't compare different age groups but can compare different groups

Select [group], sum(crude_covid_rate) as CrudeGroupRate from cov
where covid_deaths > 100
group by [group] ;


--Question 10

CREATE FUNCTION CalculateAverageCrudeCOVIDRate
(
    @JurisdictionResidence VARCHAR(60) 
)
RETURNS FLOAT
AS
BEGIN
    DECLARE @AvgCrudeCOVIDRate FLOAT

    SELECT @AvgCrudeCOVIDRate = AVG(crude_COVID_rate)
    FROM cov
    WHERE Jurisdiction_Residence = @JurisdictionResidence

    RETURN @AvgCrudeCOVIDRate
END


CREATE PROCEDURE CalculateAverageWeeklyPercentageChange
(
    @StartDate DATE,
    @EndDate DATE
)
AS
BEGIN
    SELECT
        Jurisdiction_Residence,
        @StartDate AS StartDate,
        @EndDate AS EndDate,
        AVG(((COVID_deaths - PrevWeekDeaths) * 100.0 / NULLIF(PrevWeekDeaths, 0))) AS AvgWeeklyPercentageChange
    FROM
    (
        SELECT
            Jurisdiction_Residence,
            data_period_end,
            COVID_deaths,
            LAG(COVID_deaths) OVER (PARTITION BY Jurisdiction_Residence ORDER BY data_period_end) AS PrevWeekDeaths
        FROM
            cov
        WHERE
            data_period_end BETWEEN @StartDate AND @EndDate and [group] = 'weekly'
    ) AS WeeklyChangeData
    GROUP BY
        Jurisdiction_Residence
END ;

exec CalculateAverageWeeklyPercentageChange '2023-01-01','2023-12-31' ;

select dbo.CalculateAverageCrudeCOVIDRate('alabama') ;





