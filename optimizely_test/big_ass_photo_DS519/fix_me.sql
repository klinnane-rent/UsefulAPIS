SELECT `DS497`.`date` AS `date`,
  `DS497`.`Brand` AS `Brand`,
  `DS497`.`Channel` AS `Channel`,
  `DS497`.`Device` AS `Device`,
  `DS497`.`location_category` AS `location_category`,
  `DS497`.`variant` AS `variant`,

  1 AS `sessions`, -- bounce rate
  `DS497`.`bounces` AS `bounces`,

  `DS497`.`total_leads` AS `total_leads`,
  `DS497`.`Core_total_leads` AS `Core_total_leads`, -- conversions
  `DS497`.`pin_1_3` AS `pin_1_3`,
  `DS497`.`pin_4_7` AS `pin_4_7`,
  `DS497`.`pin_8_10` AS `pin_8_10`,

  `DS497`.`Diamond` AS `Diamond`,
  `DS497`.`Platinum` AS `Platinum`,
  `DS497`.`Gold` AS `Gold`,
  `DS497`.`Silver` AS `Silver`,

  `DS497`.`lt_phone` AS `lt_phone`,
  `DS497`.`lt_email_no_intent` AS `lt_email_no_intent`,
  `DS497`.`lt_email_intent` AS `lt_email_intent`,
  `DS497`.`lt_request_tour` AS `lt_request_tour`,
  `DS497`.`lt_schedule_tour` AS `lt_schedule_tour`,

  `DS497`.`srp_views` AS `srp_views`, -- ctr
  `DS497`.`srp_pdp` AS `srp_pdp`,

   -- SRP Engagement Score
   -- (SRP-to-PDP Views + Map Pin Interactions + SRP Leads + Favorites + Photo Gallery Clicks) / (SRP Views)

  `DS497`.`total_leads_srp` AS `total_leads_srp`,
  `DS497`.`srp_saves` AS `srp_saves`,
  `DS497`.`srp_shares` AS `srp_shares`,
  `DS497`.`srp_gallery` AS `srp_gallery`,
  `DS497`.`srp_engagements` AS `srp_engagements`,
FROM `big-query-152314.ychen`.`DS497` `DS497`
