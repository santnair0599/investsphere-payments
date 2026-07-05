-- Masked views: no raw PII; granted to the right audience.

CREATE OR REPLACE VIEW investsphere.gold_masked.v_customer_masked_for_analytics AS
SELECT customer_id,
       nationality,
       status,
       source_system,
       investsphere.governance.mask_name(customer_name) AS customer_name,
       investsphere.governance.mask_email(email) AS email,
       investsphere.governance.mask_phone(phone_number) AS phone_number
FROM investsphere.gold.dim_customer;

CREATE OR REPLACE VIEW investsphere.gold_masked.v_customer_engineer AS
SELECT customer_id,
       nationality,
       status,
       source_system,
       sha2(email, 256) AS email_hash,
       sha2(phone_number, 256) AS phone_number_hash
FROM investsphere.gold.dim_customer;
