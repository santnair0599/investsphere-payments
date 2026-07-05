-- Column-mask UDFs: privileged groups see real value, others masked.

CREATE OR REPLACE FUNCTION investsphere.governance.mask_name(val STRING)
  RETURN CASE WHEN is_account_group_member('pii_approved_users') THEN val ELSE concat(left(val, 1), '***') END;

CREATE OR REPLACE FUNCTION investsphere.governance.mask_email(val STRING)
  RETURN CASE WHEN is_account_group_member('pii_approved_users') THEN val ELSE regexp_replace(val, '(^.).*(@.*$)', '$1***$2') END;

CREATE OR REPLACE FUNCTION investsphere.governance.mask_phone(val STRING)
  RETURN CASE WHEN is_account_group_member('pii_approved_users') THEN val ELSE concat('XXXXXX', right(val, 4)) END;

CREATE OR REPLACE FUNCTION investsphere.governance.mask_id(val STRING)
  RETURN CASE WHEN is_account_group_member('pii_approved_users') THEN val ELSE sha2(val, 256) END;
