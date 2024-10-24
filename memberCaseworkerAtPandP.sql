-- BigQuery SQL syntax
WITH CaseworkerChanges AS (
  SELECT
    v.id,
    v."item_id" AS "Member_ID",
    v."created_at",
    -- Remove curly braces and keep the caseworker changes string
    trim(replace(SPLIT_PART(
      TRIM(BOTH '{}' FROM v."object_changes" #>> array['caseworker_id']),
      ',', 1
    ), '[', '')) AS "Old Caseworker ID",
    trim(replace(SPLIT_PART(
      TRIM(BOTH '{}' FROM v."object_changes" #>> array['caseworker_id']),
      ',', 2
    ), ']', '')) AS "New Caseworker ID", -- Extract the new caseworker ID

    v."item_type"
  FROM
    versions v
  WHERE
    v."item_type" = 'Member'
    AND v."object_changes" #>> '{caseworker_id}' IS NOT NULL
),
LatestMemberPastPresent AS (
  SELECT DISTINCT ON (member_id)
    id,
    member_id,
    created_at
  FROM
    public.member_past_presents
  ORDER BY
    member_id, created_at DESC
),
EarliestCaseworkerChange AS (
  -- Find the earliest caseworker change after the specified point
  SELECT distinct on (lmpp."member_id")
    lmpp."member_id" as "Member_ID",
    cc."created_at" as "CaseworkerChangeCreatedAt",
    lmpp."created_at" as "PastPresent_form_at",
    cc."Old Caseworker ID" as "Old_Caseworker_ID"
  FROM
    LatestMemberPastPresent lmpp
  JOIN
    CaseworkerChanges cc ON cc."Member_ID" = lmpp."member_id"
  AND (
    (lmpp."created_at" IS NOT NULL AND cc."created_at" > lmpp."created_at")
  )
  ORDER BY
    lmpp."member_id",
    cc."created_at" ASC
),
LatestCaseworkerChange AS (
  -- Find the latest caseworker change before the specified point
  SELECT distinct on (lmpp."member_id")
    lmpp."member_id" as "Member_ID",
    cc."created_at" as "CaseworkerChangeCreatedAt",
    lmpp."created_at" as "PastPresent_form_at",
    cc."New Caseworker ID" as "New_Caseworker_ID"
  FROM
    LatestMemberPastPresent lmpp
  JOIN
    CaseworkerChanges cc ON cc."Member_ID" = lmpp."member_id"
  AND (
    (lmpp."created_at" IS NOT NULL AND cc."created_at" <= lmpp."created_at") 
  )
  ORDER BY
    lmpp."member_id",
    cc."created_at" DESC
)
SELECT
  COALESCE(e."Member_ID", l."Member_ID") AS "Member_ID",
  concat(members.first_name, ' ', members.last_name) as "Member",
  COALESCE(e."PastPresent_form_at", l."PastPresent_form_at") AS "PastPresent_form_at",
  COALESCE(e."Old_Caseworker_ID", l."New_Caseworker_ID") AS "PandP_Caseworker_ID",
  concat(cw."first_name", ' ', cw."last_name") as "PandP_Caseworker"
FROM
  EarliestCaseworkerChange e
FULL OUTER JOIN
  LatestCaseworkerChange l
ON e."Member_ID" = l."Member_ID"
JOIN members on COALESCE(e."Member_ID", l."Member_ID") = members.id
JOIN caseworkers cw ON COALESCE(e."Old_Caseworker_ID", l."New_Caseworker_ID") = cast(cw."id" as text)
ORDER BY
  COALESCE(e."Member_ID", l."Member_ID");
