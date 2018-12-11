load data inpath '/capstone/card_member_incremental' overwrite into table card_member_incremental;

load data inpath '/capstone/member_score' overwrite into table member_score;

insert into card_member_ext
select card_member_incremental.* from card_member_incremental LEFT JOIN card_member_ext ON (card_member_incremental.card_id = card_member_ext.card_id)
WHERE card_member_ext.card_id IS NULL;


insert overwrite table member_score_ext
select * from member_score;


INSERT overwrite table ranked_table select * from (select * , rank() OVER(PARTITION BY card_id ORDER BY transaction_dt DESC)rank from (select card_id, key.amount as amount , postcode , key.transaction_dt as transaction_dt from transactions_hbase where status = 'GENUINE')a
GROUP BY card_id, amount , postcode , transaction_dt)b where rank <= 10;


INSERT overwrite table wrangled_staging_tbl
Select card_id, AVG(amount), STDDEV(amount) from ranked_table group by card_id;


INSERT overwrite table ucl_calc_ext
Select card_id, (average + 3* standard_deviation) as UCL from wrangled_staging_tbl;

INSERT overwrite table lookup_interim_join
Select card_member_ext.card_id , card_member_ext.member_id , member_score_ext.score
FROM card_member_ext INNER JOIN member_score_ext ON
(card_member_ext.member_id = member_score_ext.member_id);


INSERT overwrite table lookup_joined_ext
select ucl_calc_ext.card_id , ucl_calc_ext.ucl , lookup_interim_join.score , ranked_table.postcode , ranked_table.transaction_dt
FROM ucl_calc_ext JOIN lookup_interim_join ON (ucl_calc_ext.card_id = lookup_interim_join.card_id)
JOIN ranked_table ON (ranked_table.card_id = lookup_interim_join.card_id  AND ranked_table.rank = 1);


insert overwrite table lookup_hbase
select card_id,ucl,score,postcode,transaction_dt from lookup_joined_ext;
