insert into `{{params.project_id}}.{{params.dataset}}._raw_snapshot_responses`
with all_data_distinct as
(
select 
response_id, person_id, name, email, answer, answerlabel, data, comment, note, status, dontcontact, sent, opened, responded, lastemailed, created, segment, question_type, published, publishedname, publishedavatar, deliverymethod, survey_template, theme, life_cycle, product_type_c, city_name_c, region_name_c, country_name_c, timezone_c, orderid_c, currency_c, spend_c, product_c, province_c, city_c, country_c, orders_made_c, lifetime_spend_c, customer_tags_c, shipping_c, topic_c, workflow_follow_up_blank_comm_c, kustomer_conversationid_c, workflow_kustomer_c, dashboard, email_token
from `{{params.project_id}}.{{params.dataset}}._stg_responses` union distinct
select 
response_id, person_id, name, email, answer, answerlabel, data, comment, note, status, dontcontact, sent, opened, responded, lastemailed, created, segment, question_type, published, publishedname, publishedavatar, deliverymethod, survey_template, theme, life_cycle, product_type_c, city_name_c, region_name_c, country_name_c, timezone_c, orderid_c, currency_c, spend_c, product_c, province_c, city_c, country_c, orders_made_c, lifetime_spend_c, customer_tags_c, shipping_c, topic_c, workflow_follow_up_blank_comm_c, kustomer_conversationid_c, workflow_kustomer_c, dashboard, email_token
from `{{params.project_id}}.{{params.dataset}}._raw_snapshot_responses`
),
counts_all as 
(
select response_id,count(*) count_distinct_all from all_data_distinct group by 1
),
counts_snap as 
(
  select response_id,count(*) count_distinct_all from `{{params.project_id}}.{{params.dataset}}._raw_snapshot_responses` group by 1
),
ids_to_insert as
(
select counts_all.response_id from counts_all
left join counts_snap on counts_snap.response_id = counts_all.response_id
where counts_snap.count_distinct_all <> counts_all.count_distinct_all or counts_snap.response_id is NULL
)
select rs.* from ids_to_insert
left join `{{params.project_id}}.{{params.dataset}}._stg_responses`  rs on
rs.response_id = ids_to_insert.response_id