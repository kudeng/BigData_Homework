SELECT a.sex, m.moviename, a.avgrate, a.total
  FROM
(
SELECT r.movieid, u.sex, 
       AVG(r.rate) as avgrate, 
       COUNT(r.rate) AS total
  FROM t_rating r, t_user u 
 WHERE r.userid = u.userid 
   AND u.sex = "M"
 GROUP BY r.movieid, u.sex
HAVING COUNT(rate) > 50
) AS a
  LEFT JOIN t_movie m
    ON a.movieid = m.movieid
 ORDER BY a.avgrate DESC
 LIMIT 10;



CREATE EXTERNAL TABLE `t_user`(
  `userid` int COMMENT 'from deserializer',
  `sex` string COMMENT 'from deserializer',
  `age` int COMMENT 'from deserializer',
  `occupation` string COMMENT 'from deserializer',
  `zipcode` string COMMENT 'from deserializer')
COMMENT '观众表'
ROW FORMAT SERDE
  'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES (
  'field.delim'='::')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://emr-header-1.cluster-285604:9000/user/matianxiao/data/hive/users'


CREATE EXTERNAL TABLE `t_movie`(
  `movieid` int COMMENT 'from deserializer',
  `moviename` string COMMENT 'from deserializer',
  `movietype` string COMMENT 'from deserializer')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES (
  'field.delim'='::')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://emr-header-1.cluster-285604:9000/user/matianxiao/data/hive/movies'


CREATE EXTERNAL TABLE `t_rating`(
  `userid` int COMMENT 'from deserializer',
  `movieid` int COMMENT 'from deserializer',
  `rate` int COMMENT 'from deserializer',
  `times` bigint COMMENT 'from deserializer')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES (
  'field.delim'='::')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://emr-header-1.cluster-285604:9000/user/matianxiao/data/hive/ratings'


SELECT u.age, AVG(r.rate) AS avgrate
  FROM t_rating r
  LEFT JOIN t_user u
    ON r.userid = u.userid
 WHERE movieid = 2116
 GROUP BY u.age;

SELECT u.age, AVG(r.rate) AS avgrate
  FROM t_rating r
  JOIN t_user u
    ON r.userid = u.userid
 WHERE movieid = 2116
 GROUP BY u.age;



