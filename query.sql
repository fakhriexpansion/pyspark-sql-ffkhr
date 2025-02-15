SELECT 
    ulg.id AS referral_details_id,
    ur.referral_id,
    ur.referral_source,
    CASE
        WHEN ur.referral_source = 'User Sign Up' THEN 'Online'
        WHEN ur.referral_source = 'Draft Transaction' THEN 'Offline'
        WHEN ur.referral_source = 'Lead' THEN ll.source_category
    END AS referral_source_category,
    ur.referral_at,
    ur.referrer_id,
    ul.name AS referrer_name,
    ul.phone_number AS referrer_phone_number,
    ul.homeclub AS referrer_homeclub,
    CASE WHEN ur.referral_source = 'Lead' THEN ll.source_category ELSE CONCAT(ur.referee_name, '_', ur.referee_phone) END AS referee_id,
    ur.referee_name,
    ur.referee_phone,
    urs.description AS referral_status,
    CAST(regexp_extract(rr.reward_value, '\\d+', 0) AS INT) AS num_reward_days,
    ur.transaction_id,
    pt.transaction_status,
    pt.transaction_at,
    pt.transaction_location,
    pt.transaction_type,
    ur.updated_at,
    ulg.created_at AS reward_granted_at,
    CASE 
        WHEN num_reward_days > 0 AND urs.description = "Berhasil" AND
        ur.transaction_id IS NOT NULL AND pt.transaction_status = "PAID" AND
        pt.transaction_type = "NEW" AND pt.transaction_at > ur.referral_at AND 
        YEAR(ur.referral_at) = YEAR(pt.transaction_at) AND MONTH(ur.referral_at) = MONTH(pt.transaction_at) AND 
        ul.membership_expired_date > CURRENT_DATE AND ul.is_deleted = "false" AND ulg.is_reward_granted = "TRUE" THEN 1
        WHEN (urs.description = "Menunggu" or urs.description = "Tidak Berhasil") 
        AND (num_reward_days IS NULL or num_reward_days = "null")   THEN 1
        WHEN num_reward_days > 0 AND urs.description != "Berhasil" THEN 0
        WHEN num_reward_days > 0 AND (ur.transaction_id IS NULL or ur.transaction_id = "null") THEN 0
        WHEN (num_reward_days IS NULL or num_reward_days = "null") AND ur.transaction_id IS NOT NULL AND 
        pt.transaction_status = "PAID" AND pt.transaction_at > UR.referral_at  THEN 0
        WHEN urs.description = "Berhasil" AND (num_reward_days IS NULL or num_reward_days = "null" or num_reward_days = 0) THEN 0
        WHEN pt.transaction_at < UR.referral_at THEN 0
        ELSE 0
    END AS is_business_logic_valid_int
FROM user_referrals ur
LEFT JOIN user_referrals_logs ulg ON ulg.user_referral_id = ur.referral_id
LEFT JOIN user_logs ul ON ul.user_id = ur.referrer_id
LEFT JOIN referral_rewards rr ON rr.id = ur.referral_reward_id 
LEFT JOIN lead_logs ll ON ur.referral_source = 'Lead' AND ll.lead_id = ur.referee_id
LEFT JOIN paid_transactions pt ON pt.transaction_id = ur.transaction_id
LEFT JOIN user_referral_statuses urs ON urs.id = ur.user_referral_status_id