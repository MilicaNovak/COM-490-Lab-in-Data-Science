#!/usr/bin/env python
# coding: utf-8

# # Assignment 2
# 
# The goal of this assignment is to put into action the data manipulation techniques from the previous lab ([week 5](https://dslab2019.github.io/labs/week5/)).
# 
# ## Cluster Usage
# 
# As there are many of you working with the cluster, we encourage you to prototype your queries on small data samples before running them on whole datasets.
# 
# ## Documentation
# 
# Hive queries: <https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Select>
# Hive functions: <https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF>
# 
# ## Displaying results in Zeppelin
# 
# As part of this homework, many questions ask to retrieve and display data. You should do so by using Zeppelin functionality to display tables and plots. It is part of the exercises to find out which representations are the most releveant.
# 
# ## Part I - Data from SBB/CFF/FFS (35 points)
# 
# Data source: <https://opentransportdata.swiss/en/dataset/istdaten>
# 
# In this part, you will leverage Hive to perform exploratory analysis of data published by the Open Data Platform Swiss Public Transport. (<https://opentransportdata.swiss>).
# 
# ### Dataset Description
# 
# Format: the dataset is presented a collection of textfiles with fields separated by ';' (semi-colon). There is one file per day, ranging from 13.09.2017 until 31.01.2019. We will only use a subset of that and focus on the 2017 data. The textfiles have been compressed using bzip.
# 
# Location: you can find the data on HDFS at the path `/datasets/sbb`.
# 
# Unfortunately, the full description from opentransportdata.swiss is only provided in German (<https://opentransportdata.swiss/de/cookbook/ist-daten/>). You can use an automated translator to get more information, but here are the relevant column descriptions:
# 
# - `BETRIEBSTAG`: date of the trip
# - `FAHRT_BEZEICHNER`: identifies the trip
# - `BETREIBER_ABK`, `BETREIBER_NAME`: operator (name will contain the full name, e.g. Schweizerische Bundesbahnen for SBB)
# - `PRODUCT_ID`: type of transport, e.g. train, bus
# - `LINIEN_ID`: for trains, this is the train number
# - `LINIEN_TEXT`,`VERKEHRSMITTEL_TEXT`: for trains, the service type (IC, IR, RE, etc.)
# - `ZUSATZFAHRT_TF`: boolean, true if this is an additional trip (not part of the regular schedule)
# - `FAELLT_AUS_TF`: boolean, true if this trip failed (cancelled or not completed)
# - `HALTESTELLEN_NAME`: name of the stop
# - `ANKUNFTSZEIT`: arrival time at the stop according to schedule
# - `AN_PROGNOSE`: actual arrival time (when `AN_PROGNOSE_STATUS` is `GESCHAETZT`)
# - `AN_PROGNOSE_STATUS`: look only at lines when this is `GESCHAETZT`. This indicates that `AN_PROGNOSE` is the measured time of arrival.
# - `ABFAHRTSZEIT`: departure time at the stop according to schedule
# - `AB_PROGNOSE`: actual departure time (when `AB_PROGNOSE_STATUS` is `GESCHAETZT`)
# - `AB_PROGNOSE_STATUS`: look only at lines when this is `GESCHAETZT`. This indicates that `AB_PROGNOSE` is the measured time of arrival.
# - `DURCHFAHRT_TF`: boolean, true if the transport does not stop there
# 
# Each line of the file represents a stop and contains arrival and departure times. When the stop is the start or end of a journey, the corresponding columns will be empty (`ANKUNFTSZEIT`/`ABFAHRTSZEIT`).
# In some cases, the actual times were not measured so the `AN_PROGNOSE_STATUS`/`AB_PROGNOSE_STATUS` will be empty or set to `PROGNOSE` and `AN_PROGNOSE`/`AB_PROGNOSE` will be empty.
# 
# In the repository you will also find the file `sbb_example.png` that shows how you can relate this dataset to the schedule you get from SBB's website.

# ### Question I.a. (5/35)
# 
# First, create an external table named `<your_gaspar_name>.sbb` for this dataset.
# Then create  table `<your_gaspar_name>.sbb_17_10_2017` which contains data only for the day of 17.10.2017.

# In[2]:


get_ipython().run_line_magic('jdbc', '(hive)')
create database if not exists opeter
  location '/homes/opeter/hive';

drop table if exists opeter.sbb;

create table if not exists opeter.sbb(BETRIEBSTAG string, FAHRT_BEZEICHNER string, BETREIBER_ID string, BETREIBER_ABK string, BETREIBER_NAME string, PRODUCT_ID string, LINIEN_ID int, LINIEN_TEXT string, UMLAUF_ID string, VERKEHRSMITTEL_TEXT string, ZUSATZFAHRT_TF boolean, FAELLT_AUS_TF boolean, BPUIC int, HALTESTELLEN_NAME string, ANKUNFTSZEIT string, AN_PROGNOSE string, AN_PROGNOSE_STATUS string, ABFAHRTSZEIT string, AB_PROGNOSE string, AB_PROGNOSE_STATUS string, DURCHFAHRT_TF boolean)
    row format delimited fields terminated by ';'
    stored as textfile
    location '/datasets/sbb'
    tblproperties("skip.header.line.count"="1");
    
select * from opeter.sbb limit 10;


# In[3]:


get_ipython().run_line_magic('jdbc', '(hive)')
drop table if exists opeter.sbb_17_10_2017;

create table if not exists opeter.sbb_17_10_2017
as
select * from opeter.sbb where BETRIEBSTAG='17.10.2017';

select * from opeter.sbb_17_10_2017 limit 10;


# ### Question I.b. (5/35)
# 
# i) How many stops can you find for the date 17.10.2017? Display the information by transport type (bus, train, etc.).
# Note: you can keep the German labels like 'Zug' for train.
# 
# ii) Now do the same for the whole dataset. Show and document any pattern you see.
# Hint: to properly order by date, you may have to parse them using the `unix_timestamp` function.

# NOTE:
# For data on the whole dataset in b&c, we only use data from 2017, as the whole dataset cannot be displayed due to output limitations.

# In[6]:


get_ipython().run_line_magic('jdbc', '(hive)')
select lower(min(PRODUCT_ID)) as name, count(*) as nb_trips from opeter.sbb_17_10_2017 
    where not ANKUNFTSZEIT = ""
    group by lower(PRODUCT_ID)
    order by nb_trips
    limit 10;


# In[7]:


get_ipython().run_line_magic('jdbc', '(hive)')
with stops_2017 as (
    select lower(min(PRODUCT_ID)) as PRODUCT_ID, from_unixtime(unix_timestamp(BETRIEBSTAG, "dd.MM.yyyy")) as BETRIEBSTAG, count(*) as nb_stops from opeter.sbb
        where from_unixtime(unix_timestamp(BETRIEBSTAG, "dd.MM.yyyy"), 'Y') = 2017 and not PRODUCT_ID = "" and not ANKUNFTSZEIT = ""
        group by PRODUCT_ID, BETRIEBSTAG
)
select sum(nb_stops) as nb_stops, PRODUCT_ID as name, BETRIEBSTAG
    from stops_2017
    group by PRODUCT_ID, BETRIEBSTAG
    order by BETRIEBSTAG;


# October 17th does not seem to be an outlier, the percentages of the different means of transport are almost the same. Also, we can observe a pattern of less stops on weekends and holidays, whith the contrary being true for boat stops. Also, we can see that in summer, more boat stops occur

# ### Question I.c. (5/35)
# 
# i) How many trips can you find for the date 17.10.2017? Display the information by transport type (bus, train, etc.). 
# Note: trip=full journey.
# 
# ii) Now do the same for the whole data of 2017. Is 17.10.2017 a typical day?

# In[10]:


get_ipython().run_line_magic('jdbc', '(hive)')
drop table if exists opeter.sbb_trips_17_10_2017;

--break ties with min (this will not occur)
create table if not exists opeter.sbb_trips_17_10_2017
as
select min(PRODUCT_ID) as PRODUCT_ID, FAHRT_BEZEICHNER from opeter.sbb_17_10_2017
    group by FAHRT_BEZEICHNER;

select count(FAHRT_BEZEICHNER) as nb_trips, lower(min(PRODUCT_ID)) as name
    from opeter.sbb_trips_17_10_2017
    group by lower(PRODUCT_ID)
    order by nb_trips
    limit 10;


# In[11]:


get_ipython().run_line_magic('jdbc', '(hive)')
with trips_2017 as (
    select min(PRODUCT_ID) as PRODUCT_ID, FAHRT_BEZEICHNER, from_unixtime(unix_timestamp(BETRIEBSTAG, "dd.MM.yyyy")) as BETRIEBSTAG from opeter.sbb
        where from_unixtime(unix_timestamp(BETRIEBSTAG, "dd.MM.yyyy"), 'Y') = 2017 and not PRODUCT_ID = ""
        group by FAHRT_BEZEICHNER, BETRIEBSTAG
)
select count(FAHRT_BEZEICHNER) as nb_trips, lower(min(PRODUCT_ID)) as name, BETRIEBSTAG
    from trips_2017
    group by lower(PRODUCT_ID), BETRIEBSTAG
    order by BETRIEBSTAG;


# The following observations can be made:
# - October 17th is not an outlier.
# - During the weekend, less trips occur for all types, except boat trips increase (This effect is also present on holidays (Christmas))
# - In summer, more boat trips occur than in winter.
# 
# This is in accordance to the previous question, since we can safely assume that the number of stops is highly correlated with the number of trips.

# ### Question I.d. (5/35)
# 
# How many trains per day stop at the Lausanne train station? Display the information by date and train type. Find the 3 most represented train types.
# Note: Use `VERKEHRSMITTEL_TEXT` to get the train type.
# 
# Bonus points (2.5 points): find out what happened on the two outlier days

# In[14]:


get_ipython().run_line_magic('jdbc', '(hive)')
with trips_lausanne as(
    select VERKEHRSMITTEL_TEXT, FAHRT_BEZEICHNER, from_unixtime(unix_timestamp(BETRIEBSTAG, "dd.MM.yyyy")) as BETRIEBSTAG from opeter.sbb
    where HALTESTELLEN_NAME like "%Lausanne%" and lower(PRODUCT_ID) = "zug" and from_unixtime(unix_timestamp(BETRIEBSTAG, "dd.MM.yyyy"), 'Y') = 2017
    group by FAHRT_BEZEICHNER, BETRIEBSTAG, VERKEHRSMITTEL_TEXT
)

select VERKEHRSMITTEL_TEXT, BETRIEBSTAG, count(*) as n_stops from trips_lausanne
    group by BETRIEBSTAG, VERKEHRSMITTEL_TEXT
    order by BETRIEBSTAG;


# In[15]:


get_ipython().run_line_magic('jdbc', '(hive)')
select count(*) as nb, ZUSATZFAHRT_TF from opeter.sbb
    where VERKEHRSMITTEL_TEXT = "RE" and BETRIEBSTAG = "28.10.2017" and HALTESTELLEN_NAME like "%Lausanne%"
    group by ZUSATZFAHRT_TF;


# On the 28. and 29. October 2019 there were additional trains due to construction works between Geneva and Lausanne [Source](https://company.sbb.ch/content/dam/sbb/fr/pdf/fr_sbb-konzern/fr_bunde-und-kantone/fr_regionalverkehr/fr_westschweiz/485ren_Broschuere_bf.pdf)
# The pie chart above displays the proportion of additional trains (true=additional train)

# ### Question I.e. (10/35)
# 
# Get the set of IC trains you can take to go (without connections) from Genève (main station) to Lausanne gare on a typical week day (not Saturday, not Sunday, not a bank holiday). Display the train number (`LINIEN_ID`) as well as the schedule of the trains.
# Note: do not hesitate to create intermediary tables.
# You can use the advanced search of SBB's website to check your answer, the schedule of IC1 from Genève to Lausanne has not changed.

# In[18]:


get_ipython().run_line_magic('jdbc', '(hive)')
--Get all train that stop in Geneva and Lausanne
drop table if exists opeter.geneva2lausanne;

create table if not exists opeter.geneva2lausanne
as
with genevaic as(
    select FAHRT_BEZEICHNER from opeter.sbb_17_10_2017
    where HALTESTELLEN_NAME="Genève" and linien_text = "IC" and DURCHFAHRT_TF = false
    group by FAHRT_BEZEICHNER
)
select FAHRT_BEZEICHNER from opeter.sbb_17_10_2017 as a left semi join genevaic on (a.FAHRT_BEZEICHNER = genevaic.FAHRT_BEZEICHNER)
    where HALTESTELLEN_NAME="Lausanne" and DURCHFAHRT_TF = false; 



# ##### note
# We take the data from the 17th of octobre as it is a week day, and not during any holiday, and assume that other typical days have a similar schedule

# In[20]:


get_ipython().run_line_magic('jdbc', '(hive)')
--get departure time of train in geneva
drop table if exists opeter.departgeneve;

create table if not exists opeter.departgeneve 
as
with g2l as( select * from opeter.geneva2lausanne )
select linien_id,ABFAHRTSZEIT 
    from opeter.sbb_17_10_2017 as a left semi join g2l on (a.FAHRT_BEZEICHNER = g2l.FAHRT_BEZEICHNER)
    where HALTESTELLEN_NAME="Genève";


# In[21]:


get_ipython().run_line_magic('jdbc', '(hive)')
--Get arrival time of train in Lausanne
drop table if exists opeter.arrivelausanne;

create table if not exists opeter.arrivelausanne 
as
with g2l as( select * from opeter.geneva2lausanne )
select linien_id,ANKUNFTSZEIT 
    from opeter.sbb_17_10_2017 as a left semi join g2l on (a.FAHRT_BEZEICHNER = g2l.FAHRT_BEZEICHNER)
    where HALTESTELLEN_NAME="Lausanne";


# In[22]:


get_ipython().run_line_magic('jdbc', '(hive)')
-- Create a table with departure in Geneva and arrival in Lausanne of time of trains going from Geneva to Lausanne
drop table if exists opeter.gen_lau_timetable;

create table if not exists opeter.gen_lau_timetable 
as
select depart_genenva.linien_id as id,  from_unixtime(unix_timestamp(depart_genenva.abfahrtszeit, "dd.MM.yyyy HH:mm"), 'HH:mm') as depart_geneva, from_unixtime(unix_timestamp(arrival_lausanne.ankunftszeit, "dd.MM.yyyy HH:mm"), 'HH:mm') as arrival_lausanne
from opeter.departgeneve as depart_genenva join opeter.arrivelausanne as arrival_lausanne on (depart_genenva.linien_id = arrival_lausanne.linien_id)
where (unix_timestamp(depart_genenva.abfahrtszeit, "dd.MM.yyyy HH:mm") - unix_timestamp(arrival_lausanne.ankunftszeit, "dd.MM.yyyy HH:mm")) < 0.0 ;


# In[23]:


get_ipython().run_line_magic('jdbc', '(hive)')
select id as train_id, depart_geneva as leaves_gva_at, arrival_lausanne as arrives_in_lausanne_at from opeter.gen_lau_timetable
order by depart_geneva;


# ##### Discussion 
# Checking the schedule of IC trains on the SBB website assures us that we did find the right values 
# 

# ### Question I.f. (5/35)
# 
# 
# Let's focus on train IC 711 and see if it tends to be on time.
# 
# i) Display the distribution of delays for the IC 711 train at the Lausanne train station. Display the information as a histogram with bins of size equal to one minute. How many trains have arrived more than 5 minutes late?
# Note: when the train is ahead of schedule, count this as a delay of 0.
# 
# ii) Compute the 50th and 75th percentiles of delays for IC 702, 704, ..., 728, 730 (15 trains total) at Genève main station. Which trains are the most disrupted? Can you find the tendency and interpret?

# #### Question I.f.i
# 

# In[27]:


get_ipython().run_line_magic('jdbc', '(hive)')
--Get the delay of IC 711 train over time 
drop table if exists opeter.delaylausanne;

create table if not exists opeter.delaylausanne 
as
select (case when (unix_timestamp(AN_PROGNOSE, "dd.MM.yyyy hh:mm:ss") - unix_timestamp(ANKUNFTSZEIT, "dd.MM.yyyy hh:mm")) = null then 0 when (unix_timestamp(AN_PROGNOSE, "dd.MM.yyyy hh:mm:ss") - unix_timestamp(ANKUNFTSZEIT, "dd.MM.yyyy hh:mm")) > 0.0 then (unix_timestamp(AN_PROGNOSE, "dd.MM.yyyy hh:mm:ss") - unix_timestamp(ANKUNFTSZEIT, "dd.MM.yyyy hh:mm")) else 0 end ) as delay,FAHRT_BEZEICHNER
from opeter.sbb
where linien_id = "711" and linien_text = "IC" and HALTESTELLEN_NAME = "Lausanne" ;


# In[28]:


get_ipython().run_line_magic('jdbc', '(hive)')
select round(delay/60,0) as delay, count(*) as occurence
from opeter.delaylausanne
group by delay
order by delay;


# We see that there are 2 trains that arrived with more than 5 minutes delay. 
# 

# #### Question I.f.ii

# In[31]:


get_ipython().run_line_magic('jdbc', '(hive)')
--Get the delay for IC train from 702 tp 730 at Geneva Main Station 
drop table if exists opeter.delaygeneve;

create table if not exists opeter.delaygeneve 
as
select (case when (unix_timestamp(AN_PROGNOSE, "dd.MM.yyyy hh:mm:ss") - unix_timestamp(ANKUNFTSZEIT, "dd.MM.yyyy hh:mm")) = null then 0 when (unix_timestamp(AN_PROGNOSE, "dd.MM.yyyy hh:mm:ss") - unix_timestamp(ANKUNFTSZEIT, "dd.MM.yyyy hh:mm")) > 0.0 then round((unix_timestamp(AN_PROGNOSE, "dd.MM.yyyy hh:mm:ss") - unix_timestamp(ANKUNFTSZEIT, "dd.MM.yyyy hh:mm"))/60,0) else 0 end ) as delay, linien_id
from opeter.sbb
where linien_text = "IC" and HALTESTELLEN_NAME = "Genève" and cast(linien_id as int) < 731;



# In[32]:


get_ipython().run_line_magic('jdbc', '(hive)')
--Sanity Check : Are results consistant with part one ?
select count(delay) as 711_0_delay
from opeter.delaygeneve
where linien_id = "711" and delay = 0


# ##### Note
# A sanity check was performed on the delay geneva table. We see here that for train IC 711 their is 67 train with no delay which is consistent with what was found in part i.

# In[34]:


get_ipython().run_line_magic('jdbc', '(hive)')
--0,5 and 0.75 percentile for ic train 702 to 730
select linien_id as train_id, percentile_approx(delay,0.5) as percentile_50, percentile_approx(delay,0.75) as percentile_75 
from opeter.delaygeneve
group by linien_id
order by linien_id;


# In[35]:


get_ipython().run_line_magic('jdbc', '(hive)')
--See from which station is IC 706 leaving (NB if their is no arrival time the train starts from that point)
select HALTESTELLEN_NAME as IC_706_departure_city
from opeter.sbb
where LINIEN_ID = "706" and linien_text = "IC" and ANKUNFTSZEIT = ""
group by HALTESTELLEN_NAME ;


# In[36]:


get_ipython().run_line_magic('jdbc', '(hive)')
select HALTESTELLEN_NAME as IC_707_departure_city
from opeter.sbb
where LINIEN_ID = "707" and linien_text = "IC" and ANKUNFTSZEIT = ""
group by HALTESTELLEN_NAME ;


# In[37]:


get_ipython().run_line_magic('jdbc', '(hive)')
select HALTESTELLEN_NAME IC_723_departure_city
from opeter.sbb
where LINIEN_ID = "723" and linien_text = "IC" and ANKUNFTSZEIT = ""
group by HALTESTELLEN_NAME ;


# In[38]:


get_ipython().run_line_magic('jdbc', '(hive)')
select HALTESTELLEN_NAME as IC_728_departure_city
from opeter.sbb
where LINIEN_ID = "728" and linien_text = "IC" and ANKUNFTSZEIT = ""
group by HALTESTELLEN_NAME ;


# #### Discussion
# As we can see trains that tends to be late more often, such as IC 706 or IC 728, come from far (St Gallen in this case). They will therefore tend to accumulate delay along their road. Trains that are rarely late, such as IC 707 or IC 723 will start closer (or directly from Geneva). 
# 

# ## Part II - Twitter Dataset (25 points)
# 
# Data source: <https://archive.org/details/twitterstream?sort=-publicdate>
# 
# In this part, you will leverage Hive to extract the hashtags from the source data, and then perform light exploration of the prepared data.
# 
# ### Dataset Description
# 
# Format: the dataset is presented as a collection of textfiles containing one JSON document per line. The data is organized in a hierarchy of folders, with one file per minute, ranging from 01.04.2016 until 31.07.2016. The textfiles have been compressed using bzip2.
# 
# Location: you can find the data on HDFS at the path `/datasets/twitter`.
# 
# Relevant fields:
# 
# - `created_at`, `timestamp_ms`: the first is a human-readable string representation of when the tweet was posted. The latter represents the same instant as a timestamp in seconds since UNIX epoch.
# - `lang`: the language of the tweet content
# - `entities`: parsed entities from the tweet, e.g. hashtags, user mentions, URLs
# 
# In the repository, you will also find the file `tweet-example.json` which contains a single tweet, nicely printed.
# 
# ### Disclaimer
# 
# This dataset contains unfiltered data from Twitter. As such, you may be exposed to tweets/hashtags containing vulgarities, references to sexual acts, drug usage, etc.

# ### Question II.a. (5/25)
# 
# First, create an external table named `<your_gaspar_name>.twitter` for this dataset. 
# Then create table `<your_gaspar_name>.twitter_11_08_2018` which contains data only for the day of 11.08.2018.

# In[42]:


get_ipython().run_line_magic('jdbc', '(hive)')
drop table if exists opeter.twitter_json;

create table if not exists opeter.twitter_json(json string)
  stored as textfile
  location '/datasets/twitter';


drop table if exists opeter.twitter;

create table if not exists opeter.twitter
stored as parquet
as
select
    date_format(from_utc_timestamp(from_unixtime(cast(cast(get_json_object(json, '$.timestamp_ms') as bigint)/1000 as bigint)), 'UTC'),  "yyyy-MM-dd HH:mm:ss z") as created_at,
    cast(get_json_object(json, '$.timestamp_ms')as bigint) as timestamp_ms,
    get_json_object(json, '$.lang') as lang,
    get_json_object(json, '$.entities') as entities
from opeter.twitter_json;
    
    
select * from opeter.twitter limit 10;


# In[43]:


get_ipython().run_line_magic('jdbc', '(hive)')
drop table if exists opeter.twitter_11_08_2018;

create table if not exists opeter.twitter_11_08_2018
stored as parquet
as
select * from opeter.twitter
where timestamp_ms >= 1000*unix_timestamp("2018-08-11 00:00:00", "yyyy-MM-dd HH:mm:ss") and timestamp_ms < 1000*unix_timestamp("2018-08-12 00:00:00", "yyyy-MM-dd HH:mm:ss")
order by timestamp_ms asc;


# ### Question II.b. (10/25)
# 
# Work on the `twitter_11_08_2018` table.
# 
# i) Extract the hastags array from each tweet. Filter out the rows which do not have any hashtag. Include `timestamp_ms` and `lang` in the resulting table.
# ii) Normalize the table obtained from the previous step. This means that each row should contain exactly one hashtag. Include `timestamp_ms` and `lang` in the resulting table.
# 
# Hint: your intermediary table should look like `intermediary_hashtags_example` and the final table should look like `hashtags_example` (see below).
# Hint: in (ii), you can use the following Hive functions:
# - `substr` function: <https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF#LanguageManualUDF-StringFunctions>
# - `split` function: <https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF#LanguageManualUDF-StringFunctions>
# - `explode` function: <https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF#LanguageManualUDF-explode>
#  

# In[45]:


get_ipython().run_line_magic('jdbc', '(hive)')
drop table if exists opeter.intermediary_hashtags;

create table opeter.intermediary_hashtags
stored as parquet
as
    select timestamp_ms, lang, get_json_object(entities, "$.hashtags.text") as hashtags
    from opeter.twitter_11_08_2018
    where get_json_object(entities, "$.hashtags.text") is not null;

select * from opeter.intermediary_hashtags limit 12;


# In[46]:


get_ipython().run_line_magic('jdbc', '(hive)')
drop table if exists opeter.hashtags;

create table opeter.hashtags
stored as parquet
as
    select timestamp_ms, lang, substring_index(substring_index(hashtag, '"', -2), '"', 1) as hashtag
    from opeter.intermediary_hashtags
    LATERAL VIEW explode(split(hashtags, ",")) explodeVal AS hashtag
    order by timestamp_ms asc;

select * from opeter.hashtags limit 10;


# ### Question II.c. (5/25)
# 
# i) From the hashtags table you obtained in the previous question, find out the 10 most represented languages.
# ii) Get the top 20 most mentioned hashtags. Display the results with the contribution of each language to the hashtags.

# In[48]:


get_ipython().run_line_magic('jdbc', '(hive)')
drop table if exists opeter.hashtags_lang_count;
create table opeter.hashtags_lang_count
stored as parquet
as
    select lang, count(*) as count
    from opeter.hashtags 
    group by lang
    order by count desc
    limit 10;
    
select * from opeter.hashtags_lang_count


# In[49]:


get_ipython().run_line_magic('jdbc', '(hive)')
drop table if exists opeter.hashtags_count;

create table opeter.hashtags_count
stored as parquet
as
    select hashtag, count(*) as count
    from opeter.hashtags
    group by hashtag
    order by count desc
    limit 20;

drop table if exists opeter.hashtags_count_per_lang;

create table opeter.hashtags_count_per_lang
stored as parquet
as
    select hashtag, count(*) as count, lang
    from opeter.hashtags
    group by hashtag, lang
    order by count desc;
    
drop table if exists opeter.hashtags_grouped;
create table opeter.hashtags_grouped
stored as parquet
as
    select cpl.* 
    from opeter.hashtags_count_per_lang cpl
    inner join opeter.hashtags_count hc
    on (cpl.hashtag == hc.hashtag);
    
select * from opeter.hashtags_grouped


# ### Question II.d. (5/25)
# 
# Now, working on the whole twitter dataset, perform the same as in questions II.b. and II.c.

# In[51]:


get_ipython().run_line_magic('jdbc', '(hive)')
drop table if exists opeter.intermediary_hashtags_full;

create table opeter.intermediary_hashtags_full
stored as parquet
as
    select timestamp_ms, lang, get_json_object(entities, "$.hashtags.text") as hashtags
    from opeter.twitter
    where get_json_object(entities, "$.hashtags.text") is not null;

drop table if exists opeter.hashtags_full;

create table opeter.hashtags_full
stored as parquet
as
    select timestamp_ms, lang, substring_index(substring_index(hashtag, '"', -2), '"', 1) as hashtag
    from opeter.intermediary_hashtags_full
    LATERAL VIEW explode(split(hashtags, ",")) explodeVal AS hashtag
    order by timestamp_ms asc;

select * from opeter.hashtags_full limit 10;


# In[52]:


get_ipython().run_line_magic('jdbc', '(hive)')
drop table if exists opeter.hashtags_lang_count_full;
create table opeter.hashtags_lang_count_full
stored as parquet
as
    select lang, count(*) as count
    from opeter.hashtags_full 
    group by lang
    order by count desc
    limit 10;

drop table if exists opeter.hashtags_count_full;

create table opeter.hashtags_count_full
stored as parquet
as
    select hashtag, count(*) as count
    from opeter.hashtags_full
    group by hashtag
    order by count desc
    limit 20;

drop table if exists opeter.hashtags_count_per_lang_full;

create table opeter.hashtags_count_per_lang_full
stored as parquet
as
    select hashtag, count(*) as count, lang
    from opeter.hashtags_full
    group by hashtag, lang
    order by count desc;
    
drop table if exists opeter.hashtags_grouped_full;
create table opeter.hashtags_grouped_full
stored as parquet
as
    select cpl.* 
    from opeter.hashtags_count_per_lang_full cpl
    inner join opeter.hashtags_count_full hc
    on (cpl.hashtag == hc.hashtag);
    
select * from opeter.hashtags_grouped_full


# ### Question II.e. (bonus 2.5)
# 
# Display the evolution of two hashtags of your choice. That is, display the number of mentions for each day over the range of the whole data set.
# You should use the hastag table from the previous question.

# We have found three of top 10 hashtags has one hidden factor in common, so below we will present the evolution of these hashtags and try to explain their interconnection.

# In[55]:


get_ipython().run_line_magic('jdbc', '(hive)')

with evolution_hashtags as  (
    select hashtag, date_format(from_utc_timestamp(from_unixtime(cast(timestamp_ms/1000 as bigint)), 'UTC'),  "yyyy-MM-dd") as hashtag_date from opeter.hashtags_full
)

select hashtag_date, hashtag, count(*) as count
from evolution_hashtags
where (hashtag = "IDOL" or hashtag = "MMVAs" or hashtag = "LOVE_YOURSELF")
group by  hashtag_date, hashtag
order by hashtag_date;


# Hashtag MMVAs stands for iHeartRadioMuch Music Video Awards, annual awards which gives honour and recognition for the best musical videos [iHeartRadio MMVAs](https://en.wikipedia.org/wiki/IHeartRadio_MMVAs)
# The MMVA awards have several categories, such as Video of The Year, Best Pop Artist or Group, Fan Fave Due or Group etc.
# In 2018, MMVAs awards ceremony [were held on August 26, 2018](https://en.wikipedia.org/wiki/2018_iHeartRadio_MMVAs), which can explain highest value in MMVAs trend occuring on that day.
# The non-negligible amount of mentions during month August of 2018, can be explained by the time of announcing nominations, [1st August](https://en.wikipedia.org/wiki/2018_iHeartRadio_MMVAs#cite_note-7) [source](http://exclaim.ca/music/article/heres_the_full_list_of_2018_iheartradio_mmva_nominees).
# 
# Hashtag IDOL stands for a same-named song of the south-Korean band [BTS](https://en.wikipedia.org/wiki/Idol_(BTS_song)). 
# The teaser video [was released on August 22, 2018](https://en.wikipedia.org/wiki/Idol_(BTS_song)) and [song was released on August 24, 2018](https://en.wikipedia.org/wiki/Idol_(BTS_song)), which explains almost non-existent #IDOL mentions preceeding 22nd, pick of 22nd and pick of the 24th. 
# The high number of the IDOL mentions on 24nd can be explained by the fact Idol video was, 
# [the most watched music video within the first 24 hours](https://www.billboard.com/articles/columns/pop/8472359/bts-idol-youtube-24-hour-debut), with about 56 Million watches.
# 
# If you thought there is no connection between these hashtags, sorry, you are not right. Turns out group BTS has been won the 2018 iHeartRadion MMVAs in category [Fan Fave Duo or Group](https://en.wikipedia.org/wiki/Idol_(BTS_song)). The larger number of tweets in favour of MMVAs on 26th can be understood by taking into account that fastival awarded many other artist, which produced additional MMVAs hashtags.
# 
# But that is not whole buzz around the group, LOVE_YOURSELF is hashtag that corresponds to an three album compilations Love Yourself recorded by BTS, and song Idol is part of the album [Love Yourself: Answer] (https://en.wikipedia.org/wiki/Idol_(BTS_song))
# 
# Hashtag LOVE_YOURSELF has its picks in mentions on 9th, 13th, 16th and 24th August which can be explained by release of the fictional notes related to the album on 6th, and promitional concept photos relised on 13th and 16th following different versions of album [source](https://en.wikipedia.org/wiki/Love_Yourself:_Answer). The sole album has been realised on 24th August and we can observe the Idol hashtag totally outshines the whole album by having 5 times more mentions of corresponding hashtags, and 10 times more mentions on 22nd, the day of release of song's teaser video.

# Thank you very much for your attention
