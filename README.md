/* This query is to create single record for each call */

SELECT
  t2.*,
  t1.AHT,
  t3.Call_Segment_Type_Cd AS consult_transfer_aband,
  t3.Rep_Business_Function_NM AS consult_transfer_aband_Func_name
  into SS_temp_table
FROM

/* Add AHT of all the legs of the call */
/*Step - 1 */
(SELECT DISTINCT
  call_id,
  SUM(SEGMENT_DURATION_TIME_SEC_QTY) AS AHT,
  MIN(segment_Sequence_Nbr) AS Segment_Sequence_Nbr
FROM SS_Fid_Data
GROUP BY call_id) t1
/* Step 2 */
/* Add subsequent details of the first leg of the call - one row per call */
/* The date and time are in two seperate columns. The dateadd and datediff function
 is used to combine the two columns and make a datetime column*/
/* in step 1 we have AHT for each call id, in step 2 we are adding the call segment code and business function of the first leg of the call*/

LEFT JOIN (SELECT
  Call_ID,
  IP,
  DATEADD(DAY, 0, DATEDIFF(DAY, 0, call_segment_start_date)) +
  DATEADD(DAY, 0 - DATEDIFF(DAY, 0, call_segment_start_time), call_segment_start_time) AS Datetime,
  Call_Segment_Type_CD,
  segment_Sequence_nbr,
  REP_BUSINESS_FUNCTION_NM
  FROM SS_Fid_Data) t2
  ON t1.Call_ID = t2.Call_Id
  AND t1.segment_Sequence_nbr = t2.segment_Sequence_nbr
/* Step 3 */
/* To join the 2nd leg of the call which can be transfer/consult/abandon to the first leg of the call */
/* The second leg of the call may be a transfer, consult or abandon. Attach that information to 1st leg of the call*/
LEFT JOIN (SELECT
  call_id,
  Call_Segment_Type_Cd,
  Rep_Business_Function_Nm
/* This is to find the second leg of the call */
FROM (SELECT
  call_id,
  Segment_Sequence_Nbr,
  Call_Segment_Type_Cd,
  Rep_Business_Function_NM,
  ROW_NUMBER() OVER (
  PARTITION BY call_id
  ORDER BY Segment_Sequence_Nbr) AS Row_Asc
FROM SS_Fid_Data) iq
WHERE row_asc = 2) t3
  ON t1.call_id = t3.call_id

/* FCR - 7 days FCR has been calculated by IP and Call Reason*/
   
DECLARE @beginDate date
DECLARE @endDate date

SET @beginDate = '01/01/2014'
SET @endDate = '01/01/2018'

DECLARE @loopBeginDate datetime = '01/01/2014'

/* The loop runs for each day in the given range.
a. select people who called on day 1.
b. then for customers who called on day 1 we get all the calls from those customers within 7 days
c. sort the data by ip, call reason and datetime 
d. lead checks if there is another call from same customer for same call reason then the call on day 1 is tagged as a defect. */

WHILE DATEDIFF(dd, @loopBeginDate, @endDate) >= 1
BEGIN

    DECLARE @TempANI_B TABLE (
    Call_ID nvarchar(255),
	IP nvarchar(255),
	Datetime datetime,
	Call_Segment_Type_CD nvarchar(255),
	Segment_Sequence_Nbr float,
	Rep_Business_Function_Nm nvarchar(255),
	AHT float,
	Consult_Transfer_Aband nvarchar(255),
	Consult_Transfer_Aband_Func_Name nvarchar(255),
    FirstCallDay date,
    defect int
)
  INSERT INTO @TempANI_B (Call_ID, IP, Datetime, Call_Segment_Type_CD,Segment_Sequence_Nbr, Rep_Business_Function_Nm, AHT, Consult_Transfer_Aband, Consult_Transfer_Aband_Func_Name,
  FirstCallDay, defect)
    SELECT
      t1.*
    FROM (SELECT
      tbl.*,
      FirstCallDay = @loopBeginDate,
      defect =
/*b. then for customers who called on day 1 we get all the calls from those customers within 7 days
c. sort the data by ip, call reason and datetime 
d. lead checks if there is another call from same customer for same call reason then the call on day 1 is tagged as a defect. */
                CASE
                  WHEN IP = LEAD(IP) OVER (ORDER BY IP,Rep_Business_Function_Nm, CAST(datetime AS datetime))
				  and Rep_Business_Function_Nm = lead(Rep_Business_Function_Nm) OVER (ORDER BY IP,Rep_Business_Function_Nm, CAST(datetime AS datetime)) then 1
                  ELSE 0
                END
    FROM SS_Temp_table tbl
    WHERE cast(datetime as date) >= @loopBeginDate
    AND cast(datetime as date)  <= DATEADD(dd, 7, @loopBeginDate)) t1
	/* Step a - select all the customers who called on day 1*/
    JOIN (SELECT DISTINCT
      IP, Rep_Business_Function_nm
    FROM SS_Temp_table
    WHERE cast(datetime as date) = @loopBeginDate) t2

      ON t1.IP = t2.IP

  SET @loopBeginDate = DATEADD(dd, 1, @loopBeginDate)
END

SELECT
  * --into SS_temp2
FROM @TempANI_B 
WHERE FirstCallDay = CAST(datetime as date)

select * from SS_temp2

/* AHT by Time of the day */

select cast(CONVERT(char(17), (DATEADD(mi, DATEDIFF(mi, 0, Datetime) / 30 * 30, 0)), 113) AS Time), avg(AHT)
from SS_Temp2
group by cast(CONVERT(char(17), (DATEADD(mi, DATEDIFF(mi, 0, Datetime) / 30 * 30, 0)), 113) AS Time)

/* AHT by Business Function */
select Rep_Business_Function_NM, avg(AHT)
from SS_temp2
group by Rep_Business_Function_NM

/* FCR by Business Function - can be grouped by customer id */
select Rep_Business_Function_NM, sum(defect), count(*)
from SS_temp2
group by Rep_Business_Function_NM

/* AHT for consult, transfer, abandoned and regular calls - can be grouped by date as well */
select consult_transfer_aband, avg(aht)
from SS_temp2
group by Consult_Transfer_Aband

/* Total transfers, abandon, consults by date -can be grouped by business type as well */

select FirstCallDay, sum(aband), sum(consult), sum(transfer), count(*) from 
(
select *, aband = case when consult_transfer_aband = 'ABANDONED' then 1 else 0 end,
consult = case when consult_transfer_aband = 'CONSULT' then 1 else 0 end,
transfer = case when consult_transfer_aband = 'TRANSFER' then 1 else 0 end
from ss_temp2) t1
group by FirstCallDay




,dc2018_web_data_ip,tot_valid_visit,avg_stay_time,sameday_repeat_visit,null_visit
0,19379,3.0,660.0,1.0,
1,47022,2.0,2520.0,1.0,14.0
2,83065,39.0,858.461538462,24.0,3.0
3,85130,,,,1.0
4,218571,7.0,317.142857143,2.0,
5,482569,3.0,940.0,0.0,2.0
6,490528,23.0,571.304347826,3.0,6.0
